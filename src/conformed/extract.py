'''This module is the extract layer
 of the application'''


def ratings_full_load(delta_df):
    ''' Function to cast the columns when read in full mode
    Parameters : dataframe
    return : dataframe
    '''

    final_df=delta_df.selectExpr("cast(userId as int)","cast(movieId as int)","cast(rating as double)","from_unixtime(timestamp) as timestamp")

    return  final_df


def movies_full_load(delta_df):
    ''' Function to cast the columns when read in full mode
    Parameters : dataframe
    return : dataframe
    '''

    final_df=delta_df.selectExpr("cast(movieId as int)","cast(title as string)","cast(genres as string)")

    return  final_df



def ratings_delta_load(conf_df, delta_df, column_list):
    '''Function to perform delta logic with the new file and the existing
    Parameters : existing dataframe, new arrived dataframe, join column list
    return : dataframe
    '''
    # Update matched records
    conf_df_cache=conf_df.cache()
    if conf_df.limit(1).count() > 0:
        upd_old_ratings_df = delta_df.alias('delta_df').join(conf_df_cache.alias('conf_df'),column_list).selectExpr("cast(delta_df.userId as int)","cast(delta_df.movieId as int)","cast(delta_df.rating as double)","from_unixtime(delta_df.timestamp) as timestamp")

    # get_new_records for delta
        delta_new_df = delta_df.alias('delta_df').join(conf_df_cache.alias('conf_df'),column_list,"left_anti").selectExpr("cast(delta_df.userId as int)","cast(delta_df.movieId as int)","cast(delta_df.rating as double)","from_unixtime(delta_df.timestamp) as timestamp ")

        upd_new_ratings_df = upd_old_ratings_df.union(delta_new_df).dropDuplicates()

    #unmatched records from delta

        conf_cur_df = conf_df_cache.alias('conf_df').join(delta_df.alias('delta_df'),column_list,"left_anti").selectExpr("cast(conf_df.userId as int)","cast(conf_df.movieId as int)","cast(conf_df.rating as double)","from_unixtime(conf_df.timestamp)as timestamp")
    # union unmatched delta to prefinal dataset
        final_df = conf_cur_df.union(upd_new_ratings_df).dropDuplicates()
    else:
        final_df=delta_df

    return  final_df

def movies_delta_load(conf_df, delta_df, column_list):
    '''Function to perform delta logic with the new file and the existing
    Parameters : existing dataframe, new arrived dataframe, join column list
    return : dataframe
    '''
    # Update matched records
    conf_df_cache = conf_df.cache()
    if conf_df.limit(1).count() > 0:
        upd_old_ratings_df = delta_df.alias('delta_df').join(conf_df_cache.alias('conf_df'),column_list).selectExpr("cast(delta_df.movieId as int)","cast( delta_df.title as string)","cast(delta_df.genres as string)")

    # get_new_records for delta
        delta_new_df = delta_df.alias('delta_df').join(conf_df_cache.alias('conf_df'),column_list,"left_anti").selectExpr("cast(delta_df.movieId as int)","cast(delta_df.title as string)","cast(delta_df.genres as string)")

        upd_new_ratings_df = upd_old_ratings_df.union(delta_new_df).dropDuplicates()

    #unmatched records from delta

        conf_cur_df = conf_df.alias('conf_df').join(delta_df.alias('delta_df'),column_list,"left_anti").selectExpr("cast(conf_df.movieId as int)","cast(conf_df.title as string)","cast(conf_df.genres as string)")
    # union unmatched delta to prefinal dataset
        final_df = conf_cur_df.union(upd_new_ratings_df).dropDuplicates()
    else:
        final_df=delta_df

    return  final_df
'''
def tags_delta_load(conf_df, delta_df, column_list):

    # Update matched records

    if conf_df.limit(1).count() > 0:
        upd_old_ratings_df = delta_df.alias('delta_df').join(conf_df.alias('conf_df'),column_list).selectExpr("cast(delta_df.userId as int)","cast(delta_df.movieId as int)","cast(delta_df.tag as string)","from_unixtime(delta_df.timestamp) as timestamp")

    # get_new_records for delta
        delta_new_df = delta_df.alias('delta_df').join(conf_df.alias('conf_df'),column_list,"left_anti").selectExpr("cast(delta_df.userId as int)","cast(delta_df.movieId as int)","cast(delta_df.tag as string)","from_unixtime(delta_df.timestamp) as timestamp")

        upd_new_ratings_df = upd_old_ratings_df.union(delta_new_df).dropDuplicates()

    #unmatched records from delta

        conf_cur_df = conf_df.alias('conf_df').join(delta_df.alias('delta_df'),column_list,"left_anti").selectExpr("cast(conf_df.userId as int)","cast(conf_df.movieId as int)","cast(conf_df.tag as string)","from_unixtime(conf_df.timestamp) as timestamp")
    # union unmatched delta to prefinal dataset
        final_df = conf_cur_df.union(upd_new_ratings_df).dropDuplicates()
    else:
        final_df=delta_df

    return  final_df
'''