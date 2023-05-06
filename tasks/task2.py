from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class TaskTwo:
    def __init__(self, path=None, session=None):
        self.path = path
        self.session = session or self.start_session()
        self.input_data = self._read_path()

    def start_session(self):
        spark_session = None
        try:
            spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
        except Exception as error:
            print("Task 2 Error. Can not start Spark Session")
            print(error)

        return spark_session

    def _read_path(self):
        movies_df = None
        try:
            movies_df = self.session.read.csv(self.path, header=True, sep='\t')
        except Exception as error:
            print("Task 2 Error. Can not read input data file")
            print(error)

        return movies_df

    def get_data(self):
        result = None
        try:
            result = self.input_data.filter(f.col('birthYear') < '1900')
        except Exception as error:
            print("Task 2 Error. Can not filter input data")
            print(error)

        return result

    def show_table(self, data=None):
        data.select('primaryName', 'birthYear').show()
