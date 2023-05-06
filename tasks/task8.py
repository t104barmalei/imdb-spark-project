from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f
from pprint import pprint

class TaskEight:
    def __init__(self, path=None,films_rating_path=None, session=None):
        self.path = path
        self.films_rating_path = films_rating_path
        self.session = session or self.start_session()
        self.films_df = self._read_path()
        self.films_reiting_df = self._read_path1()


    def start_session(self):
        spark_session = None
        try:
            spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
        except Exception as error:
            print("Task 8 Error. Can not start Spark Session")
            print(error)

        return spark_session

    def _read_path(self):
        movies_df = None
        try:
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
            movies_df = self.session.read.csv(self.path,film_schema, header=True, sep='\t')
            movies_df.filter(f.col('startYear').isNotNull())
        except Exception as error:
            print("Task 8 Error. Can not read input data file")
            print(error)

        return movies_df

    def _read_path1(self):
        movies_df1 = None
        try:
            movies_df1 = self.session.read.csv(self.films_rating_path, header=True, sep='\t')

        except Exception as error:
            print("Task 8 Error. Can not read input data file")
            print(error)

        return movies_df1

    def get_data(self):
        result = None
        try:
            result_df = self.films_df.join(self.films_reiting_df, self.films_df['tconst'] == self.films_reiting_df['tconst']). \
                select(self.films_df['originalTitle'], self.films_reiting_df['averageRating'], self.films_df['genres']).filter(
                f.col('genres').isNotNull()).filter(f.col('genres') != '\\N') \
                .sort(self.films_df['genres'], self.films_reiting_df['averageRating'].desc())

            explod_df = result_df.withColumn('genres', f.split(result_df['genres'], ',').getItem(0))
            # explod_df.select('originalTitle', f.explode('genres'))
            # explod_df.show()

            print("Convert to pandas")
            result_df_pd = explod_df.toPandas()
            print("Convert to list")
            a = list(set(result_df_pd['genres'].tolist()))
            print(a)
            print("Start cycle For")
            genres_all = list()
            cnt = 0
            for str_full in a:
                cnt += 1
                print(f"Iteration {cnt}")
                lst_tmp = str_full.split(',')
                genres_all = genres_all + lst_tmp

            genres_all = list(set(genres_all))
            pprint(genres_all)

            pdf_set = list()
            for genre in genres_all:
                pdf = explod_df.filter(f.col('genres').like('%' + genre + '%')).sort(
                    f.col('averageRating').desc()).limit(10)
                pdf_set.append(pdf)

            res_cnt = 0
            for i in pdf_set:
                res_cnt += 1
                print(f"Start concatinate: {res_cnt}")
                a = i
                if res_cnt == 1:
                    result = a
                    continue

                result = result.union(a)

        except Exception as error:
            print("Task 8 Error. Can not filter input data")
            print(error)

        return result

    def show_table(self, data=None):
        data.show()