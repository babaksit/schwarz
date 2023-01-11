import json
from pyspark.sql import SparkSession
from src.utils.spark import SparkHandler
import matplotlib.pyplot as plt

"""
The analysis part also could be done using classes
"""

def run_sql_queries(spark_handler: SparkHandler) -> None:
    """
    Run analysis sql queries and print the results
    Args:
        spark_handler (SparkHandler): An instance of SparkHandler class

    Returns:
        None:
    """
    sql_root_path = "/home/bsi/projects/schwarz/src/etl/sql/analysis/"
    sql_files = ["temp_num_rides.sql", "day_num_rides.sql",
                 "hour_num_rides.sql", "dayofweek_num_rides.sql",
                 "bike_type_percentage.sql" , "user_type_percentage.sql"]
    for sql_file in sql_files:
        path = sql_root_path + sql_file
        df = spark_handler.spark.sql(open(path).read())
        df.show()


def plot_lat_lon(spark_handler: SparkHandler):
    """
    Plot latitude and longitude of positions
    Args:
        spark_handler (SparkHandler): An instance of SparkHandler class

    Returns:
        None:
    """
    df = spark_handler.read_table("citibike_weather_merged")

    df = df.toPandas()
    # Create a figure and a subplot
    _, ax = plt.subplots()

    # Plot the start coordinates on the map
    df.plot(kind='scatter', x='start_lng', y='start_lat', ax=ax, color='blue')

    # Plot the end coordinates on the map
    df.plot(kind='scatter', x='end_lng', y='end_lat', ax=ax, color='red')

    # Display the plot
    plt.show()


if __name__ == '__main__':
    with open('../etl/config/pipeline/citibike.json', 'r') as f:
        citibike_pipeline_config = json.load(f)

    spark = SparkSession.builder.config("spark.driver.extraClassPath", citibike_pipeline_config["spark"]["driver_path"]) \
        .appName(citibike_pipeline_config["spark"]["application_name"]).getOrCreate()

    spark_handler = SparkHandler(spark, citibike_pipeline_config["postgres"]["url"],
                                 citibike_pipeline_config["postgres"]["user_name"],
                                 citibike_pipeline_config["postgres"]["password"],
                                 citibike_pipeline_config["postgres"]["database_name"])
    spark_handler.read_table("citibike_weather_merged").createOrReplaceTempView("citibike_weather_merged")
    run_sql_queries(spark_handler)
    plot_lat_lon(spark_handler)
