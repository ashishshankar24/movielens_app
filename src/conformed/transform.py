'''This module is the transformation module
 contains all the tranformation functions'''
from pyspark.sql.functions import *
from pyspark.sql import Window



def split_genres(moviesdf):
    '''Function to split the genres for each movies into multiple rows
    Parameters : raw dataframe
    returns : transformed dataframe
    '''
    df = moviesdf.withColumn("genres", explode(split(moviesdf.genres, "[|]")))
    return df


def avg_ratings(ratingsdf):
    '''Function to find the avg ratings of the movies which has aleast 5 ratings and diplay top 10 movie ratigs
    Parameters : raw dataframe
    returns : transformed dataframe
    '''
    windowspec = Window.partitionBy("movieId").orderBy(desc("rating"))
    window_spec = Window.partitionBy("movieId")
    ratings_row_num = ratingsdf.withColumn("row_number",row_number().over(windowspec))
    ratings_count_5 = ratings_row_num.filter("row_number >= 5").select("movieId").distinct()

    ratings_avg = ratingsdf.join(broadcast(ratings_count_5), ["movieId"]).withColumn('avg_rating',avg(col('rating')).over(window_spec)).selectExpr("movieId", "cast(avg_rating as decimal(3,2))").distinct().orderBy(desc("avg_rating"), desc("movieId"))

    top_rating_df = ratings_avg.limit(10)

    return top_rating_df