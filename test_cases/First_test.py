import unittest
from pandas.testing import assert_frame_equal
from resources.spark_session import get_spark
from resources.config_reader import config_reader
import pandas as pd

class Testing_First(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
            print("Started Test Class: {}".format(cls.__name__))
            cls.spark_config = config_reader.getSparkConfig()
            cls.spark = get_spark.getSparkSession(cls.spark_config)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        print("Stopped Test Class: {}".format(cls.__name__))

    def test_integer_compare_1(self):
            self.assertEqual(10,10)

    def test_string_compare_1(self):
            self.assertEqual("omkar","omkar")

    def test_compare_spark_panda_dataframe(self):
            data = [["Java", "20000"], ["Python", "100000"], ["Scala", "3000"]]
            columns = ["language","users_count"]
            # Create the spark DataFrame
            spark_dataframe = self.spark.createDataFrame(data).toDF(*columns)
            # Create the pandas DataFrame
            panda_dataframe = pd.DataFrame(data,columns=columns)
            assert_frame_equal(panda_dataframe.sort_values("users_count").reset_index(drop=True),spark_dataframe.na.fill("").orderBy("users_count").toPandas().reset_index(drop=True))
