"""
spark.py
~~~~~~~~
Module containing spark core functions
"""
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from os import  path
import json
from dependencies import logging
from pathlib import Path
def initiate_spark(app_name='movielens_app', master='local[*]'):
    spark_builder = (
        SparkSession
            .builder
            .master(master)
            .appName(app_name))


    # create session


    spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.sql.shuffle.partitions", 50)
    spark_logger = logging.Log4j(spark_session)
    # get config file if sent to cluster with --files
    spark_files_dir = SparkFiles.getRootDirectory()
    config_files = "app_config.json"

    path_to_config_file = path.join(spark_files_dir, config_files)
    with open(path_to_config_file, 'r') as config_file:
        config_dict = json.load(config_file)
        spark_logger.info('loaded config from : app_config.json')

    return spark_session,config_dict