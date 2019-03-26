""""
Spark ETL to extract top ten movies in each category per
decade
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pysparl.sql.functions as fun

from etl_conf import conf

def load_data(spark_session):
    sc = spark_session.sparkContext
    movie_file = sc.textFile('/spark_etl/data/ml-10m/ml-10M100K/movies.dat')
    movie_file = movie_file.map(lambda l: l.split("::"))

    movie_df = movie_file.toDF(("movie_id", "movie_name", "genre"))

    rating_file = sc.textFile('/spark_etl/data/ml-10m/ml-10M100K/ratings.dat')
    rating_file = rating_file.map(lambda l: l.split("::"))

    ratings_df = rating_file.toDF(
        ("user_id", "movie_id", "rating", "time_stamp")).drop('user_id')

    return movies_df, ratings_df


def main():
    spark_session = SparkSession.builder.appName("TopMoviesPerDecade")\
        .getOrCreate()

    movies_df, ratings_df = load_data(spark_session)

    ratings_decade_wise = ratings_df.withColumn('decade',
                                            func.floor(func.year(
                                            func.from_unixtime('time_stamp')\
                                            .cast(DateType())) /10)*10)\
                                            .drop('time_stamp')

    movie_data_tmp = movie_data.drop('movie_name')

    ratings_w_movies = ratings_decade_wise.join(broadcast(movie_data_tmp),
                                   ratings_decade_wise.movie_id == movie_data_tmp.movie_id,
                                   how='left').drop(
        movie_data_tmp.movie_id)

    ratings_w_movies = res.withColumn('categories', func.explode(
        func.split(res["genre"], "\\|"))).drop(res['genre', 'rating'])

    ratings_agg = ratings_w_movies.groupBy("year", "categories", "movie_id").agg(
        {'categories': 'count'}).withColumnRenamed('count(categories)', 'freq')

    window_spec = Window.partitionBy("year", "categories").orderBy(
        func.desc("freq"))

    ratings_agg = ratings_agg.withColumn("rank", func.rank().over(w))

    top10 = ratings_agg.where(top10["rank"] <= 10)

    top10.show()

if __name__ == '__main__':
    ENV = os.getenv('ENV', 'DEV')
    conf = conf[ENV]
    main(conf)