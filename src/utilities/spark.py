import logging

from pyspark.sql.utils import AnalysisException


class SparkHandler:
    def __init__(self, spark, db_url, user_name, password, database_name):
        self._spark = spark
        self._password = password
        self._user_name = user_name
        self._database_name = database_name
        self._db_url = db_url

    @property
    def spark(self):
        return self._spark

    def read_table(self, table_name):
        # TODO change to more secure way of connecting. e.g. using ssh keys ...
        try:
            return self.spark.read.format("jdbc") \
                .option("url", f"{self._db_url}{self._database_name}") \
                .option("dbtable", table_name) \
                .option("user", self._user_name) \
                .option("password", self._password) \
                .load()
        except AnalysisException as e:
            logging.error(e)
            return None

    def write_table_error_if_exists(self, data, table_name):
        # TODO change to more secure way of connecting. e.g. using ssh keys ...
        try:
            data.write.format("jdbc") \
                .option("url", f"{self._db_url}{self._database_name}") \
                .option("dbtable", table_name) \
                .option("user", self._user_name) \
                .option("password", self._password) \
                .mode("errorIfExists") \
                .save()
        except AnalysisException as e:
            logging.error(e)
