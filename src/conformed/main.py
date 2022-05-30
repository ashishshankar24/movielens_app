import os.path
import sys
from configparser import ConfigParser
sys.path.insert(0, 'C:\Users\Ashish\PycharmProjects\Movielens\src')


from utils.sparkUtils import *
from dependencies.spark import initiate_spark
from dependencies  import logging
from conformed.extract import *
from conformed.transform import *



def main(mode):


    spark,config = initiate_spark("movieslens", "local[*]")

    conf_path = config['conf_path']
    input_path = config['input_path']
    spark_logger = logging.Log4j(spark)
    spark_logger.info("Initiating Program...")

# Extraction call in Full mode, this process will run only one time after production deployment
# Step1: Read the 1st file from the source location
# Step2: return the dataframe read , function created as decorator
# Step4: Writing dataframe back to table location


    spark_logger.info("Executing Extraction ")
    if (mode == 'full'):
        ratings_full_df = read_csv(spark, "ratings", input_path)
        spark_logger.info("Ratings file read completed for from source location")
        ratings_stage_df = ratings_full_load(ratings_full_df)
        write(ratings_stage_df, "ratings", conf_path)
        spark_logger.info("Ratings data loaded to table location")

        movies_full_df = read_csv(spark, "movies", input_path)
        spark_logger.info("Movies file read completed for from source location")
        movies_stage_df = movies_full_load(movies_full_df)
        write(movies_stage_df, "movies", conf_path)
        spark_logger.info("Movies data loaded to table location")

    #Tags development is not part of first iteration and will be released in future release,Hence code is commented
      #  tags_stage_df = full_load(tags_full_df)
      #  write(tags_stage_df, "tags", conf_path)
      #  tags_full_df = delta_read(spark, "tags", delta_path)

# Extraction call in Delta mode, this is daily execution mode.
# Step1: reading conformed data after full from the base table location
# Step2: reading daily arrived file
# Step3 : applying delta logic to find updated and new records
# Step4: Writing dataframe back to table location

    elif (mode == 'delta'):
        # stage load for ratings
        column_list = ["userId", "movieId"]
        ratings_conf_df = conf_read(spark, "ratings", conf_path)
        spark_logger.info("Ratings table read from conformed location completed successfully")

        ratings_delta_df = read_csv(spark, "ratings", input_path)
        spark_logger.info("Daily Ratings file read completed successfully")

        ratings_stage_df = ratings_delta_load(ratings_conf_df, ratings_delta_df, column_list)
        ratings_stage_df.cache().count()
        write(ratings_stage_df, "ratings", conf_path)
        spark_logger.info("Delta data write to Conformed location completed successfully")

        column_list = ["movieId"]
        movies_conf_df = conf_read(spark, "movies", conf_path)
        spark_logger.info("Ratings table read from conformed location completed successfully")

        movies_delta_df = read_csv(spark, "movies", input_path)
        spark_logger.info("Daily Ratings file read completed successfully")

        movies_stage_df = movies_delta_load(movies_conf_df, movies_delta_df, column_list)
        movies_conf_df.cache().count()
        write(movies_stage_df, "movies", conf_path)
        spark_logger.info("Delta data write to Conformed location completed successfully")


        #Tags development is not part of first iteration and will be released in future release,Hence code is commented
        #column_list = ["movieId"]
        #tags_conf_df = conf_read(spark, "tags", conf_path)
        #tags_delta_df = read_csv(spark, "tags", delta_path)
        #tags_stage_df = tags_delta_load(tags_conf_df, tags_delta_df, column_list)
        #tags_conf_df.cache().count()
        #write(tags_stage_df, "tags", conf_path)


    spark_logger.info("Executing Transformations ")
    m = split_genres(movies_stage_df) # Split the genres column to multiple rows
    r = avg_ratings(ratings_stage_df) # determine the top 10 moves with highest avg ratings

    # Write the data to single csv file
    write_csv(m,"split_genres",conf_path)
    spark_logger.info("Data with Splitting Genres written to conformed location successfully")

    write_csv(r,"avg_ratings",conf_path)
    spark_logger.info("Data with Average ratings written to conformed location successfully")

    spark_logger.info("Process completed successfully")
if __name__ == '__main__':
    main(sys.argv[1])


