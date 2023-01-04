from pyspark.sql import SparkSession

from src.etl.extract.file.extract_csv import PostgresCSVDataExtract
from src.etl.transform.cleansing.template import CitibikeDataCleaningWithSpark, WeatherDataCleaningWithSpark
from src.etl.transform.processing.process import ProcessCitiBikeWeatherDataWithSpark
from src.utilities.spark import SparkHandler

spark = SparkSession.builder.config("spark.driver.extraClassPath", "/home/bsi/Downloads/postgresql-42.5.1.jar") \
    .appName("CitibikeWeatherAnalysis").getOrCreate()

spark_handler = SparkHandler(spark, "jdbc:postgresql://127.0.0.1:5432/", "admin", "admin", "schwarz")

citibike_pcd = PostgresCSVDataExtract(spark_handler
                                      ,
                                      '/home/bsi/projects/schwarz/src/data/raw/citibike/JC-202211-citibike-tripdata.csv'
                                      , "citibike_interim")
citibike_pcd.extract()
weather_pcd = PostgresCSVDataExtract(spark_handler,
                                     '/home/bsi/projects/schwarz/src/data/raw/weather/nyc 2022-11-01 to 2022-11-30.csv'
                                     , "weather_interim")
weather_pcd.extract()


citibike_pcd_df = spark_handler.read_table("citibike_interim")
weather_pcd_df = spark_handler.read_table("weather_interim")
citibike_cleaning = CitibikeDataCleaningWithSpark(spark_handler, "citibike_cleaned")
citibike_cleaned = citibike_cleaning.clean(citibike_pcd_df)
weather_cleaning = WeatherDataCleaningWithSpark(spark_handler, "weather_cleaned")
weather_cleaned = weather_cleaning.clean(weather_pcd_df)

process = ProcessCitiBikeWeatherDataWithSpark(
    spark_handler, ['weather_cleaned', 'citibike_cleaned']
    , '/home/bsi/projects/schwarz/src/etl/sql/citibike_weather/join/join_on_hour.sql'
    , "citibike_weather_merged")

process.execute()
