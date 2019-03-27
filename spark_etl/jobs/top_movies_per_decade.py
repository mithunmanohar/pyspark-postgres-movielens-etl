""""
Spark ETL to extract top ten movies in each category per
decade
"""
import os
import sys
print(sys.path.insert(0, '/spark_etl'))

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import *
import pyspark.sql.functions as func


from etl_conf import conf

def load_data(spark_session):
    sc = spark_session.sparkContext
    movie_file = sc.textFile('/spark_etl/data/ml-10m/ml-10M100K/movies.dat')
    movie_file = movie_file.map(lambda l: l.split("::"))

    movies_df = movie_file.toDF(("movie_id", "movie_name", "genre"))

    rating_file = sc.textFile('/spark_etl/data/ml-10m/ml-10M100K/ratings.dat')
    rating_file = rating_file.map(lambda l: l.split("::"))

    ratings_df = rating_file.toDF(
        ("user_id", "movie_id", "rating", "time_stamp")).drop('user_id')

    return movies_df, ratings_df


def main(conf):
    spark_session = SparkSession.builder.appName("TopMoviesPerDecade")\
        .getOrCreate()

    movies_df, ratings_df = load_data(spark_session)

    ratings_decade_wise = ratings_df.withColumn('decade',
                                            func.floor(func.year(
                                            func.from_unixtime('time_stamp')\
                                            .cast(DateType())) /10)*10)\
                                            .drop('time_stamp')

    movie_data_tmp = movies_df.drop('movie_name')

    ratings_w_movies = ratings_decade_wise.join(func.broadcast(movie_data_tmp),
                                   ratings_decade_wise.movie_id == movie_data_tmp.movie_id,
                                   how='left').drop(
        movie_data_tmp.movie_id)

    ratings_w_movies = ratings_w_movies.withColumn('categories', func.explode(
        func.split(ratings_w_movies["genre"], "\\|"))).drop('genre', 'rating')

    ratings_agg = ratings_w_movies.groupBy("decade", "categories", "movie_id").agg(
        {'categories': 'count'}).withColumnRenamed('count(categories)', 'freq')

    window_spec = Window.partitionBy("decade", "categories").orderBy(
        func.desc("freq"))

    ratings_agg = ratings_agg.withColumn("rank", func.rank().over(window_spec))

    top10 = ratings_agg.where(ratings_agg["rank"] <= 10)

    top10.show(100)
    categories = [('Crime', 1), ('Romance', 2), ('Thriller', 3),
                  ('Adventure', 4), ('Drama', 5), ('War', 6),
                  ('Documentary', 7), ('Fantasy', 8), ('Mystery', 9),
                  ('Musical', 10), ('Animation', 11), ('Film-Noir', 12),
                  ('(no genres listed)', 13), ('IMAX', 14),
                  ('Horror', 15), ('Western', 16), ('Comedy', 17),
                  ('Children', 18), ('Action', 19), ('Sci-Fi', 20)]
    category_df = spark_session.createDataFrame(categories,
                                                ['categories',
                                                 'category_id'])
    top10 = top10.join(func.broadcast(category_df), ['categories'])
    movie_data = movies_df.drop('genre')
    top10 = top10.join(func.broadcast(movie_data), ['movie_id'],
                       how='left').drop('categories', 'freq')
    top10 = top10.withColumnRenamed('r', 'rank')
    
    print(top10.show(1000))
    pg_cred = conf.pg_db['pg_data_lake']

    #top10.write.jdbc(
    #    url="jdbc:" + pg_cred['host'] + '/' + pg_cred['dbname'],
    #    table="t_movie_rank",
    #    mode="overwrite",
    #     properties={
     #       "user": pg_cred['user'],
      #      "password": pg_cred['password'],
       #     "driver": "org.postgresql.Driver"
        #}
  #  )


if __name__ == '__main__':
    ENV = os.getenv('ENV', 'DEV')
    conf = conf[ENV]
    main(conf)
