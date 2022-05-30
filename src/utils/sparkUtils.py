'''This module contains all common functions
 used in the application'''
import time
from os import path
ts = time.gmtime()
partition_ts = time.strftime("%Y%m%d", ts)


def read_csv(spark, tablename, file_path):
    ''' Function to read the new csv files that arrive in the source location
    Parameters : spark session, file base name, source location
    return : dataframe
    '''
    table_path = file_path + '/' + tablename + '.csv'
    if path.exists(table_path):
        df = spark.read.option("inferSchema","true").option("header","true").csv(table_path)
    else:
        print("Delta file not arrives yet, exiting with failure")
        exit(1)
    return df



def conf_read(spark, tablename, file_path):
    ''' Function to read the conformed table after full load is performed
    Parameters : spark session, table name, conformed data location
    return : dataframe
    '''
    table_path = file_path + "/" + tablename + "/" + "*"

    df = spark.read.parquet(table_path)

    return df



def write(df, tablename, file_path):
    '''Function to write the dataframe to the table location
    Parameters : dataframe, table name, conformed data location
    return : None
    '''
    table_path = file_path + '/' + tablename + '/' + partition_ts
    df.write.mode("Overwrite").parquet(table_path)


def write_csv(df, filename, file_path):
    '''Function to write the transformes to csv file
    Parameters : dataframe, file_name, conformed data location
    return : None
    '''
    table_path = file_path + '/' + filename
    df.coalesce(1).write.mode("Overwrite").csv(table_path,header=True)

'''
def delta_load(conf_df, delta_df, column_list, ):

    # Update matched records

    if conf_df.limit(1).count() > 0:
        upd_old_ratings_df = delta_df.join(conf_df,column_list).select("cast(conf_df.userId as bigint)","cast(conf_df.movieId as bigint)","cast(conf_df.rating as float)","from_unixtime(conf_df.timestamp) as timestamp")

    # get_new_records for delta
        delta_new_df = delta_df.join(conf_df,column_list,"left_anti").select("cast(delta_df.userId as bigint)","cast(delta_df.movieId as bigint)","cast(delta_df.rating as float)","from_unixtime(delta_df.timestamp) ")

        upd_new_ratings_df = upd_old_ratings_df.union(delta_new_df).dropduplicates()

    #unmatched records from delta

        conf_cur_df = conf_df.join(delta_df,column_list,"left_anti").select("cast(conf_df.userId as bigint)","cast(conf_df.movieId as bigint)","cast(conf_df.rating as float)","from_unixtime(conf_df.timestamp)")
    # union unmatched delta to prefinal dataset
        final_df = conf_cur_df.union(upd_new_ratings_df).dropduplicates
    else:
        final_df=delta_df

    return  final_df


'''