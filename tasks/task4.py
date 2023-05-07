from pyspark import SparkConf
from pyspark.sql import SparkSession


class TaskFour:
    def __init__(self, path=None, path1=None, path2=None, session=None, output_path=None):
        self.path = path
        self.path1 = path1
        self.path2 = path2
        self.session = session or self.start_session()
        self.output_path = output_path
        self.result = None

        self.input_data = self._read_path()
        self.input_data1 = self._read_path1()
        self.input_data2 = self._read_path2()

    def start_session(self):
        spark_session = None
        try:
            spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
        except Exception as error:
            print("Task 4 Error. Can not start Spark Session")
            print(error)

        return spark_session

    def _read_path(self):
        movies_df = None
        try:
            movies_df = self.session.read.csv(self.path, header=True, sep='\t')
        except Exception as error:
            print("Task 4 Error. Can not read input data file")
            print(error)

        return movies_df

    def _read_path1(self):
        movies_df1 = None
        try:
            movies_df1 = self.session.read.csv(self.path1, header=True, sep='\t')
        except Exception as error:
            print("Task 4 Error. Can not read input data file")
            print(error)

        return movies_df1

    def _read_path2(self):
        movies_df2 = None
        try:
            movies_df2 = self.session.read.csv(self.path2, header=True, sep='\t')
        except Exception as error:
            print("Task 4 Error. Can not read input data file")
            print(error)

        return movies_df2

    def get_data(self):
        result = None
        try:
            result = self.input_data.join(self.input_data1, on='nconst', how='left'). \
                join(self.input_data2, self.input_data1["tconst"] == self.input_data2["titleId"], "inner"). \
                select(self.input_data['primaryName'], self.input_data1['characters'],
                       self.input_data2["title"]).filter(
                self.input_data1['category'] == 'actor')
        except Exception as error:
            print("Task 4 Error. Can not filter input data")
            print(error)

        return result

    def write_results(self, file_name: str = "task4.csv"):
        try:
            self.result.write.option('encoding', 'Windows-1251').csv(self.output_path + '\\' + file_name, header=True, mode='overwrite')
        except Exception as error:
            print("Error! Can not write Results File of Task4")
            print(error)

    def show_table(self, data=None):
        self.result = data
        self.result.show()
