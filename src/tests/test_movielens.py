
import unittest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from decimal import Decimal
from datetime import datetime

import sys
sys.path.insert(0, 'C:\Users\Ashish\PycharmProjects\Movielens\src\conformed')


from transform import *
from extract import *
class MyTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local[*]")
                     .appName("Movielens-Unit-tests")
                     .config('spark.port.maxRetries', 30)
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_delta_load_ratings(self):
            # 1. Prepare an input data frame that mimics our source data.
        input_schema = StructType([
                StructField('userId', IntegerType(), True),
                StructField('movieId', IntegerType(), True),
                StructField('rating', IntegerType(), True),
                StructField('timestamp', LongType(), True)
            ])
        conf_data = [(1, 1, 4, 964982703), (2, 1, 5, 964982703),(3, 1, 5, 964982703)]
        conf_df = self.spark.createDataFrame(data=conf_data, schema=input_schema)

        delta_data = [(1, 1, 5, 964982092), (4, 6, 5, 964982703)]
        delta_df = self.spark.createDataFrame(data=delta_data, schema=input_schema)

        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
                StructField('userId', IntegerType(), True),
                StructField('movieId', IntegerType(), True),
                StructField('rating', IntegerType(), True),
                StructField('timestamp', StringType(), True)
            ])
        expected_data = [(1, 1, 5, datetime.fromtimestamp(964982092).strftime('%Y-%m-%d %H:%M:%S')), (2, 1, 5, datetime.fromtimestamp(964982703).strftime('%Y-%m-%d %H:%M:%S')), (3, 1, 5,datetime.fromtimestamp(964982703).strftime('%Y-%m-%d %H:%M:%S')), (4, 6, 5,datetime.fromtimestamp(964982703).strftime('%Y-%m-%d %H:%M:%S'))]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # 3. Apply our transformation to the input data frame
        delta_load_df = ratings_delta_load(conf_df, delta_df, ["userId", "movieId"])

        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(delta_load_df.collect()))

    def test_delta_load_movies(self):
        # 1. Prepare an input data frame that mimics our source data.
        input_schema = StructType([
            StructField('movieId', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('genres', StringType(), True)

        ])
        conf_data = [(1, "Toy Story (1995)", "Adventure|Animation|Children|Comedy|Fantasy"),
                      (2, "Jumanji (1995)", "Adventure|Children|Fantasy"),
                     (3,"Grumpier Old Men (1995)","Comedy|Romance")]
        conf_df = self.spark.createDataFrame(data=conf_data, schema=input_schema)

        delta_data = [(1, "Toy Story (1996)", "Adventure|Animation|Children|Comedy|Fantasy"),
                     (4, "Harry porter", "Adventure|Children|Fantasy")]
        delta_df = self.spark.createDataFrame(data=delta_data, schema=input_schema)

        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
            StructField('movieId', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('genres', StringType(), True)
        ])

        expected_data = [(1, "Toy Story (1996)", "Adventure|Animation|Children|Comedy|Fantasy"),
                         (2, "Jumanji (1995)", "Adventure|Children|Fantasy"),
                         (3,"Grumpier Old Men (1995)","Comedy|Romance"),
                         (4, "Harry porter", "Adventure|Children|Fantasy")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # 3. Apply our transformation to the input data frame
        delta_load_df = movies_delta_load(conf_df,delta_df,["movieId"])

        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(delta_load_df.collect()))

    def test_split_genres(self):
        # 1. Prepare an input data frame that mimics our source data.
        input_schema = StructType([
            StructField('movieId', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('genres', StringType(), True)

        ])
        input_data = [(1, "Toy Story", "Adventure|Animation|Children|Comedy|Fantasy"),
                      (2, "Tom and Huck (1995)", "Adventure|Children")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        # 2. Prepare an expected data frame which is the output that we expect.
        expected_schema = StructType([
            StructField('movieId', IntegerType(), True),
            StructField('title', StringType(), True),
            StructField('genres', StringType(), True)
        ])

        expected_data = [(1, "Toy Story", "Adventure"),
                         (1, "Toy Story", "Animation"),
                         (1, "Toy Story", "Children"),
                         (1, "Toy Story", "Comedy"),
                         (1, "Toy Story", "Fantasy"),
                         (2, "Tom and Huck (1995)", "Adventure"),
                         (2, "Tom and Huck (1995)", "Children")]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # 3. Apply our transformation to the input data frame
        transformed_df = split_genres(input_df)

        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    def test_avg_calc(self):
            # 1. Prepare an input data frame that mimics our source data.
            input_schema = StructType([
                StructField('userId', IntegerType(), True),
                StructField('movieId', IntegerType(), True),
                StructField('rating', IntegerType(), True),
                StructField('timestamp', LongType(), True)

            ])
            input_data = [(1, 1, 4, 964982703), (2, 1, 5, 964982703), (3, 1, 5, 964982703),
                          (1, 2, 4, 964982703), (2, 2, 3, 964982703), (3, 2, 5, 964982703), (4, 2, 4, 964982703),
                          (5, 2, 3, 964982703), (6, 2, 3, 964982703),
                          (1, 3, 4, 964982703), (2, 3, 3, 964982703), (3, 3, 5, 964982703), (4, 3, 5, 964982703),
                          (5, 3, 2, 964982703), (6, 3, 2, 964982703),
                          (1, 4, 4, 964982703), (2, 4, 5, 964982703), (3, 4, 5, 964982703), (4, 4, 2, 964982703),
                          (5, 4, 2, 964982703), (6, 4, 2, 964982703),
                          (1, 5, 4, 964982703), (2, 5, 5, 964982703), (3, 5, 5, 964982703), (4, 5, 2, 964982703),
                          (5, 5, 2, 964982703), (6, 5, 4, 964982703),
                          (1, 6, 4, 964982703), (2, 6, 5, 964982703), (3, 6, 5, 964982703), (4, 6, 2, 964982703),
                          (5, 6, 5, 964982703), (6, 6, 4, 964982703),
                          (1, 7, 4, 964982703), (2, 7, 1, 964982703), (3, 7, 5, 964982703), (4, 7, 1, 964982703),
                          (5, 7, 2, 964982703), (6, 7, 4, 964982703),
                          (1, 8, 4, 964982703), (2, 8, 1, 964982703), (3, 8, 5, 964982703), (4, 8, 1, 964982703),
                          (5, 8, 1, 964982703), (6, 8, 1, 964982703),
                          (1, 9, 4, 964982703), (2, 9, 1, 964982703), (3, 9, 5, 964982703), (4, 9, 3, 964982703),
                          (5, 9, 5, 964982703), (6, 9, 1, 964982703),
                          (1, 10, 4, 964982703), (2, 10, 2, 964982703), (3, 10, 5, 964982703), (4, 10, 4, 964982703),
                          (5, 10, 3, 964982703), (6, 10, 1, 964982703),
                          (1, 11, 5, 964982703), (2, 11, 5, 964982703), (3, 11, 5, 964982703), (4, 11, 5, 964982703)]
            input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
            # 2. Prepare an expected data frame which is the output that we expect.
            expected_schema = StructType([
                StructField('movieId', IntegerType(), True),
                StructField('avg_rating', DecimalType(3,2), True)

            ])

            expected_data = [(6, Decimal(4.17)),
                             (5, Decimal(3.67)),
                             (2, Decimal(3.67)),
                             (3, Decimal(3.5)),
                             (4, Decimal(3.33)),
                             (10, Decimal(3.17)),
                             (9, Decimal(3.17)),
                             (7, Decimal(2.83)),
                             (8, Decimal(2.17))]
            expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

            # 3. Apply our transformation to the input data frame
            transformed_df = avg_ratings(input_df)

            # Compare data in transformed_df and expected_df
            self.assertEqual(transformed_df.collect(),expected_df.collect())

if __name__ == '__main__':
    unittest.main()

'''
        # 4. Assert the output of the transformation to the expected data frame.
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        # Compare schema of transformed_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
'''

