import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType

def data_quality(s3_bucket, filename, columns):
    spark = SparkSession.builder.appName("Data quality").getOrCreate()

    df = spark.read.load(s3_bucket + filename, header=True, inferSchema=True, format='parquet')

    for column in columns:
        num_nulls = df.where(F.col(column).isNull()).count()
        if num_nulls > 0:
            raise ValueError("Data quality check failed. Column: {} returned {} nulls".format(column, num_nulls))

if __name__ == "__main__":
    s3_bucket = "s3a://dend-jose/"
    filename = 'I94_data_transformed_2016.parquet'
    columns = ['arrival_date', 'origin_airport_code', 'stay_state']

    data_quality(s3_bucket, filename, columns)
