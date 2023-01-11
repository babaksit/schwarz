from pyspark.sql import SparkSession

from src.etl.extract.file.extract_csv import PostgresCSVDataExtract
from src.etl.transform.cleansing.datasets.citibike import CitibikeDataCleaningWithSpark
from src.etl.transform.cleansing.datasets.weather import WeatherDataCleaningWithSpark
from src.etl.transform.processing.process import ProcessCitiBikeWeatherDataWithSpark
from src.shared.parameters.citibike_params import CitibikeParams
from src.shared.parameters.weather_params import WeatherParams
from src.utils.spark import SparkHandler

import json

with open('./etl/config/pipeline/citibike.json', 'r') as f:
    citibike_pipeline_config = json.load(f)

spark = SparkSession.builder.config("spark.driver.extraClassPath", citibike_pipeline_config["spark"]["driver_path"]) \
    .appName(citibike_pipeline_config["spark"]["application_name"]).getOrCreate()

spark_handler = SparkHandler(spark, citibike_pipeline_config["postgres"]["url"],
                             citibike_pipeline_config["postgres"]["user_name"],
                             citibike_pipeline_config["postgres"]["password"],
                             citibike_pipeline_config["postgres"]["database_name"])

with open('./etl/config/extract/citibike.json', 'r') as f:
    citibike_extract_config = json.load(f)
citibike_pcd = PostgresCSVDataExtract(spark_handler,
                                      citibike_extract_config["file_path"]
                                      , citibike_extract_config["table_name"])
citibike_pcd.extract()

with open('./etl/config/extract/weather.json', 'r') as f:
    weather_extract_config = json.load(f)
weather_pcd = PostgresCSVDataExtract(spark_handler,
                                     weather_extract_config["file_path"]
                                     , weather_extract_config["table_name"])
weather_pcd.extract()

citibike_pcd_df = spark_handler.read_table(citibike_extract_config["table_name"])
weather_pcd_df = spark_handler.read_table(weather_extract_config["table_name"])

with open('./etl/config/transform/citibike.json', 'r') as f:
    citibike_transform_config = json.load(f)
with open('./etl/config/transform/weather.json', 'r') as f:
    weather_transform_config = json.load(f)

with open('./etl/config/params/citibike.json', 'r') as f:
    citibike_param_config = json.load(f)
with open('./etl/config/params/weather.json', 'r') as f:
    weather_param_config = json.load(f)

CitibikeParams.read(citibike_param_config)
WeatherParams.read(weather_param_config)


citibike_cleaning = CitibikeDataCleaningWithSpark(spark_handler, citibike_transform_config["table_name"])
citibike_cleaning.clean(citibike_pcd_df)
weather_cleaning = WeatherDataCleaningWithSpark(spark_handler, weather_transform_config["table_name"])
weather_cleaning.clean(weather_pcd_df)


with open('./etl/config/transform/citibike_weather_merge.json', 'r') as f:
    merge_config = json.load(f)

process = ProcessCitiBikeWeatherDataWithSpark(
    spark_handler, [merge_config["citibike_table"], merge_config["weather_table"]]
    , merge_config["sql_path"]
    , merge_config["table_name"]
)
process.execute()
