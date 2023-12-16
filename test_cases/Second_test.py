import unittest
from pyspark.sql.functions import col,regexp_replace
from pandas.testing import assert_frame_equal
from resources.spark_session import get_spark
from resources.config_reader import config_reader
from pyspark.sql.types import StructType, StructField, ArrayType, DoubleType,StringType

class Testing_Second(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
            print("Started Test Class: {}".format(cls.__name__))
            cls.spark_config = config_reader.getSparkConfig()
            cls.spark = get_spark.getSparkSession(cls.spark_config)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        print("Stopped Test Class: {}".format(cls.__name__))

    def test_string_compare(self):
        self.assertEqual("omkar","omkar")

    def test_not_equal_string(self):
        self.assertNotEqual("18","20")

    def remove_extra_spaces(self,df, column_name):
        # Remove extra spaces from the specified column using regexp_replace
        df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))
        return df_transformed

    def test_compare_spark_panda_dataframe(self):
        sample_data = [{"name": "John    D.", "age": 30},
                       {"name": "Alice   G.", "age": 25},
                       {"name": "Bob  T.", "age": 35},
                       {"name": "Eve   A.", "age": 28}]
        # Create a Spark DataFrame
        original_df = self.spark.createDataFrame(sample_data)
        # Apply the transformation to remove space
        transformed_df = self.remove_extra_spaces(original_df, "name")

        expected_data = [{"name": "John D.", "age": 30},
        {"name": "Alice G.", "age": 25},
        {"name": "Bob T.", "age": 35},
        {"name": "Eve A.", "age": 28}]

        expected_df = self.spark.createDataFrame(expected_data)
        print("Original:")
        original_df.show()
        print("transformed")
        transformed_df.show()
        print("Expected")
        expected_df.show()
        assert_frame_equal(transformed_df.na.fill("").orderBy("age").toPandas().reset_index(drop=True), expected_df.na.fill("").orderBy("age").toPandas().reset_index(drop=True))

    def test_schema(self):
        s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
        self.assertEqual(s1, s2)

    def test_not_equalschema(self):
        s1 = StructType([StructField("names", ArrayType(DoubleType(), True), True)])
        s2 = StructType([StructField("names", ArrayType(StringType(), True), True)])
        self.assertEqual(s1, s2)