import logging
from abc import abstractmethod, ABC

from pyspark.sql.utils import AnalysisException

from src.utilities.spark import SparkHandler


class Processor(ABC):
    def execute(self):
        self.read()
        data = self.process()
        self.save(data)

    @abstractmethod
    def read(self):
        raise NotImplementedError

    @abstractmethod
    def process(self):
        raise NotImplementedError

    @abstractmethod
    def save(self, data):
        raise NotImplementedError


class SparkProcess(Processor):
    pass


class ProcessCitiBikeWeatherDataWithSpark(SparkProcess):

    def __init__(self, spark_handler, tables, query_path, result_table_name):
        self.result_table_name = result_table_name
        self.spark_handler = spark_handler
        self.tables = tables
        self.query_path = query_path

    def read(self):
        for table in self.tables:
            self.spark_handler.read_table(table).createOrReplaceTempView(table)

    def process(self):
        ret = None
        try:
            ret = self.spark_handler.spark.sql(open(self.query_path).read())
        except AnalysisException as e:
            logging.error(e)
        return ret

    def save(self, data):
        self.spark_handler.write_table_error_if_exists(data, self.result_table_name)
