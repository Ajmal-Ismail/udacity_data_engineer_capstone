import configparser
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import *


# Parse project configurations
config = configparser.ConfigParser()
config.read('cp.cfg')

SAS_LABELS_DESCRIPTION_FILE_PATH=config.get('PATHS', 'SAS_LABELS_DESCRIPTION_FILE_PATH')
IMMIGRATION_DATA_PATH=config.get('PATHS', 'IMMIGRATION_DATA_PATH')
DEMOGRAPHIC_DATA_PATH=config.get('PATHS', 'DEMOGRAPHIC_DATA_PATH')
OUTPUT_PATH=config.get('PATHS', 'OUTPUT_PATH')


def parse_sas_labels(data_type):
    """
        This is a helper function to parse various data labels from
        description file provided with the immigration dataset.
    """
    with open(SAS_LABELS_DESCRIPTION_FILE_PATH) as file:
        file_contents = file.read()
    data = file_contents[file_contents.index(data_type):]
    data = data[:data.index(';')]

    lines = data.split('\n')
    codes, values = [], []
    for line in lines:
        parts = line.split('=')
        if len(parts) != 2:
            continue
        codes.append(parts[0].strip().strip("'"))
        values.append(parts[1].strip().strip("'"))
    return codes, values


def get_i94_modes(spark):
    """
        This function loads i94 modes data from immigration data description file.
    """
    codes, values = parse_sas_labels('I94MODE')
    return spark.createDataFrame(pd.DataFrame(list(zip(codes, values)), columns=['code', 'mode']))


def get_i94_visas(spark):
    """
        This function loads i94 visa types data from immigration data description file.
    """
    codes, values = parse_sas_labels('I94VISA')
    return spark.createDataFrame(pd.DataFrame(list(zip(codes, values)), columns=['code', 'visa_type']))


def get_i94_states(spark):
    """
        This function loads i94 states data from immigration data description file.
    """
    codes, values = parse_sas_labels('I94ADDR')
    return spark.createDataFrame(pd.DataFrame(list(zip(codes, values)), columns=['code', 'state_name']))


def get_i94_countries(spark):
    """
        This function loads countries data from immigration data description file.
    """
    codes, values = parse_sas_labels('I94CIT')
    return spark.createDataFrame(pd.DataFrame(list(zip(codes, values)), columns=['code', 'country_name']))


def get_i94_ports(spark):
    """
        This function loads i94 ports data from immigration data description file.
    """
    codes, values = parse_sas_labels('I94PORT')
    return spark.createDataFrame(pd.DataFrame(list(zip(codes, values)), columns=['code', 'port_name']))


def get_demographics_data(spark):
    """
        This function loads demographics data into spark dataframe with appropriate data types
        and also orverrides the columns names in the original CSV file to match the convention with 
        other datasets.
    """
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", StringType()),
        StructField("female_population", StringType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("number_of_foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())])
    return spark.read.csv(DEMOGRAPHIC_DATA_PATH, header=True, sep=';', schema=schema)


def get_immigration_data(spark):
    """
        This function loads immigration data.
    """
    return spark.read.format('com.github.saurfang.sas.spark').load(IMMIGRATION_DATA_PATH)


def clean_states_data(df):
    """
        This function cleans up the state codes and removes invalid state code. Any invalid state codes will be null
        in the final table.
    """
    return df.filter('code <> "99"')


def clean_countries_data(df):
    """
        This function cleans up the countries data parsed from the description file. It updates any invalid country names
        with `NA` using a generic regex expression.
    """
    return df.withColumn('country_name', regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'NA'))


def clean_ports_data(df):
    """
        Ports data in the description file is combination of city and state. This function splits the port name into
        two seperate columns, city and state which would be helpful in join this with other datasets.
    """
    return df.withColumn('city', trim(split(col('port_name'), ',').getItem(0)))\
        .withColumn('state_code', trim(split(col('port_name'), ',').getItem(1)))\
        .drop('port_name')


def clean_immigration_data(df):
    """
        This function parse arrival date in immigration data.
    """
    get_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    return df.withColumn('arrdate', get_date(df.arrdate))


def clean_demographics_data(df):
    """
        This function removes all the records where there is no state data exists.
    """
    return df.filter('state_code IS NOT NULL')


def create_immigration_fact_table(spark, immigration_df, us_states_df, visa_types_df, modes_df, us_ports_df, countries_df):
    immigration_df.createOrReplaceTempView('tbl_immigration_data')
    us_states_df.createOrReplaceTempView('lu_states')
    visa_types_df.createOrReplaceTempView('lu_visa_type')
    modes_df.createOrReplaceTempView('lu_modes')
    us_ports_df.createOrReplaceTempView('lu_ports')
    countries_df.createOrReplaceTempView('lu_countries')
    
    return spark.sql("""
        SELECT
            tid.i94yr AS year,
            tid.i94mon AS month,
            lc.code AS residence_country,
            lp.code AS port,
            tid.arrdate AS arrival_date,
            lm.code AS mode,
            ls.code AS state_code,
            tid.depdate AS departure_date,
            tid.i94bir AS age,
            lvt.code AS visa_type_code,
            tid.occup AS occupation,
            tid.gender AS gender,
            tid.biryear AS birth_year,
            tid.dtaddto AS allowed_date,
            tid.airline AS airline,
            tid.admnum AS admission_number,
            tid.fltno AS flight_number,
            tid.visatype AS visa_type
        FROM tbl_immigration_data tid
            LEFT JOIN lu_states ls ON ls.code = tid.i94addr
            LEFT JOIN lu_visa_type lvt ON lvt.code = tid.i94visa
            LEFT JOIN lu_modes lm ON lm.code = tid.i94mode
            LEFT JOIN lu_ports lp ON lp.code = tid.i94port
            LEFT JOIN lu_countries lc ON lc.code = tid.i94res
        WHERE 
            lp.code IS NOT NULL AND
            lc.code IS NOT NULL AND
            lm.code IS NOT NULL AND
            ls.code IS NOT NULL AND
            lvt.code IS NOT NULL
    """)


def create_port_demographics_dim_table(spark, demographics_df, us_ports_df):
    """
        This function aggregates the demographics data based on city and state and then map that data with
        i94port code that can be used to reference this data from immigration dataset.
    """
    demographics_df.createOrReplaceTempView('tbl_demographics')
    us_ports_df.createOrReplaceTempView('lu_ports')
    
    aggregated_df = spark.sql("""
        SELECT
            td.city,
            td.state_code,
            SUM(td.male_population) AS total_male_population,
            SUM(td.female_population) AS total_female_population,
            SUM(td.total_population) AS total_population,
            SUM(td.number_of_veterans) AS number_of_veterans,
            SUM(td.number_of_foreign_born) AS number_of_foreign_born
        FROM tbl_demographics td
        GROUP BY td.city, td.state_code
    """)
    aggregated_df.createOrReplaceTempView('tbl_demographics')
    return spark.sql("""
        SELECT
            lp.code AS port_code,
            td.*
        FROM lu_ports lp
            JOIN tbl_demographics td ON lower(td.city) = lower(lp.city) AND td.state_code = lp.state_code
    """)


def main():
    # create spark session
    spark = SparkSession.builder.\
        config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
        .enableHiveSupport().getOrCreate()
    
    # load all the datasets into spark data frames
    immigration_df = get_immigration_data(spark)
    demographics_df = get_demographics_data(spark)
    ports_df = get_i94_ports(spark)
    countries_df = get_i94_countries(spark)
    states_df = get_i94_states(spark)
    modes_df = get_i94_modes(spark)
    visa_types_df = get_i94_visas(spark)
    
    # clean up individual datasets
    cleaned_immigration_df = clean_immigration_data(immigration_df)
    cleaned_demographics_df = clean_demographics_data(demographics_df)
    cleaned_ports_df = clean_ports_data(ports_df)
    cleaned_countries_df = clean_countries_data(countries_df)
    cleaned_states_df = clean_states_data(states_df)
    
    # create fact table for immigration data
    immigration_fact_table = create_immigration_fact_table(spark, cleaned_immigration_df, cleaned_states_df, visa_types_df, modes_df, cleaned_ports_df, cleaned_countries_df)
    
    # create port demographics dimension table
    port_demographics_table = create_port_demographics_dim_table(spark, cleaned_demographics_df, cleaned_ports_df)
    
    # data quality checks
    if immigration_fact_table.count() == 0:
        Exception("Something went wrong. No rows returned for immigration table.")
        
    if port_demographics_table.count() == 0:
        Exception("Something went wrong. No rows returned for demographics table.")
    
    # write tables in parquet format
    immigration_fact_table.write.mode('overwrite').partitionBy('year', 'month', 'state_code').parquet(OUTPUT_PATH + "immigrations")
    port_demographics_table.write.mode('overwrite').partitionBy('state_code').parquet(OUTPUT_PATH + "port_demographics")
    modes_df.write.mode('overwrite').parquet(OUTPUT_PATH + "mode")
    visa_types_df.write.mode('overwrite').parquet(OUTPUT_PATH + "visa_type")
    cleaned_states_df.write.mode('overwrite').parquet(OUTPUT_PATH + "state")
    cleaned_countries_df.write.mode('overwrite').parquet(OUTPUT_PATH + "country")
    cleaned_ports_df.write.mode('overwrite').parquet(OUTPUT_PATH + "port")
    

if __name__ == '__main__':
    main()