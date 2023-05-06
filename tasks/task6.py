from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t
import pyspark.sql.functions as f


class TaskSix:
    def __init__(self, path=None,films_episode_df_path=None, session=None):
        self.path = path
        self.films_episode_df_path = films_episode_df_path
        self.session = session or self.start_session()
        self.films_df = self._read_path()
        self.films_episode_df = self._read_path1()


    def start_session(self):
        spark_session = None
        try:
            spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())
        except Exception as error:
            print("Task 6 Error. Can not start Spark Session")
            print(error)

        return spark_session

    def _read_path(self):
        movies_df = None
        try:
            movies_df = self.session.read.csv(self.path, header=True, sep='\t')
        except Exception as error:
            print("Task 6 Error. Can not read input data file")
            print(error)

        return movies_df

    def _read_path1(self):
        movies_df1 = None
        try:
            episode_schema = t.StructType([
                t.StructField("tconst", t.StringType(), False),
                t.StructField("parentTconst", t.StringType(), False),
                t.StructField("seasonNumber", t.StringType(), False),
                t.StructField("episodeNumber", t.IntegerType(), False)])
            movies_df1 = self.session.read.csv(self.films_episode_df_path, episode_schema, header=True, sep='\t')
        except Exception as error:
            print("Task 6 Error. Can not read input data file")
            print(error)

        return movies_df1

    def get_data(self):
        result = None
        try:
            a = self.films_episode_df.filter(self.films_episode_df['episodeNumber'].isNotNull())
            result = self.films_df.join(a, self.films_df['tconst'] == a['parentTconst'])\
                .select(self.films_df['originalTitle'], a['episodeNumber']).groupBy('originalTitle')\
                .agg(f.count('episodeNumber').alias('epNumber')).orderBy(['epCount'], ascending=False).limit(50)

        except Exception as error:
            print("Task 6 Error. Can not filter input data")
            print(error)

        return result

    def show_table(self, data=None):
        data.show()
