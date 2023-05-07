from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
import math


class TaskSeven:
    def __init__(self, path=None, films_rating_path=None, session=None, output_path=None):
        self.path = path
        self.films_rating_path = films_rating_path
        self.session = session or self.start_session()
        self.films_df = self._read_path()
        self.films_rating_df = self._read_path1()
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
            print("Task 7 Error. Can not start Spark Session")
            print(error)

        return spark_session

    def _get_schema(self):
        film_schema = t.StructType([
            t.StructField("tconst", t.StringType(), False),
            t.StructField("titleType", t.StringType(), False),
            t.StructField("primaryTitle", t.StringType(), False),
            t.StructField("originalTitle", t.StringType(), False),
            t.StructField("isAdult", t.StringType(), False),
            t.StructField("startYear", t.IntegerType(), False),
            t.StructField("endYear", t.IntegerType(), False),
            t.StructField("runtimeMinutes", t.IntegerType(), False),
            t.StructField("genres", t.StringType(), False)])

        return film_schema

    def _read_path(self):
        movies_df = None
        try:
            movies_df = self.session.read.csv(self.path, self._get_schema(), header=True, sep='\t')
            movies_df.filter(f.col('startYear').isNotNull())
        except Exception as error:
            print("Task 7 Error. Can not read input data file")
            print(error)

        return movies_df

    def _read_path1(self):
        movies_df1 = None
        try:
            movies_df1 = self.session.read.csv(self.films_rating_path, header=True, sep='\t')
        except Exception as error:
            print("Task 7 Error. Can not read input data file")
            print(error)

        return movies_df1

    def get_data(self):
        result = None
        try:
            result_df = self.films_df.\
                join(self.films_rating_df, self.films_df['tconst'] == self.films_rating_df['tconst']). \
                select(self.films_df['originalTitle'], self.films_rating_df['averageRating'],
                       self.films_df['startYear']). \
                sort(self.films_df['startYear'], self.films_rating_df['averageRating'].desc()).\
                filter(f.col('startYear').isNotNull())

            result_df_max = result_df.select(f.max(result_df['startYear']))
            result_df_min = result_df.select(f.min(result_df['startYear']))

            result_df_max = (math.trunc(result_df_max.head()[0] / 10) + 1) * 10
            result_df_min = math.trunc(result_df_min.head()[0] / 10) * 10

            step = 10
            res_list = []
            for decade in range(result_df_min, result_df_max + 1, step):
                tmp = result_df.filter(f.col('startYear') >= decade).filter(f.col('startYear') < decade + step). \
                    limit(10)

                res_list.append(
                    tmp.withColumnRenamed('startYear', 'Decade').sort(f.col('averageRating').desc())
                    .withColumn('Decade', f.lit(f"{decade}-{decade + step}"))
                )

            res_cnt = 0
            for films_decades in res_list:
                res_cnt += 1
                current_value = films_decades
                if res_cnt == 1:
                    result = current_value
                    continue

                result = result.union(current_value)

        except Exception as error:
            print("Task 7 Error. Can not filter input data")
            print(error)

        return result

    def write_results(self, file_name: str = "task7.csv"):
        try:
            self.result.write.option('encoding', 'Windows-1251').csv(self.output_path + '\\' + file_name, header=True, mode='overwrite')
        except Exception as error:
            print("Error! Can not write Results File of Task7")
            print(error)

    def show_table(self, data=None):
        self.result = data
        self.result.show()
