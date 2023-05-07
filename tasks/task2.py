from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


class TaskTwo:
    def __init__(self, path=None, session=None, output_path=None):
        self.path = path
        self.session = session or self.start_session()
        self.input_data = self._read_path()
        self.output_path = output_path
        self.result = None

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

    def write_results(self, file_name: str = "task2.csv"):
        try:
            self.result.write.option('encoding', 'Windows-1251').csv(self.output_path + '\\' + file_name, header=True, mode='overwrite')
        except Exception as error:
            print("Error! Can not write Results File of Task2")
            print(error)

    def show_table(self, data=None):
        self.result = data.select('primaryName', 'birthYear')
        self.result.show()
