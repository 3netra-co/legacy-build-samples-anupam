
import boto3
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import col,lit, lpad
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

def clear_s3_bucket_path(bucket_name, prefix):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=prefix):
        obj.delete()

clear_s3_bucket_path("attestrds", "mstloan/etl")


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define the schema for the DataFrame
address_schema = StructType([
    StructField("ACCOUNT_NUMBER", StringType(), True),
    StructField("STANDARDIZED_ADDRESS", StringType(), True),
    StructField("LAT", DoubleType(), True),
    StructField("LONG", DoubleType(), True)
])



input_path = "S3 path to your loan file.csv"
df = spark.read.option("header", "true").csv(input_path)


df = (df
      .withColumn("Property_Address", col("Property_Address").cast("string"))
      .withColumn("Property_City", col("Property_City").cast("string"))
      .withColumn("Property_State", col("Property_State").cast("string"))
      .withColumn("Property_Zip", lpad(col("Property_Zip"), 5, "0").cast("string"))
      )
      

def standardize_addresses_in_partition(records):
    # Initialize the client here so it's done once per partition, not per record
    location_client = boto3.client('location')
    for record in records:
        try:
            full_address = f"{record['Property_Address']}, {record['Property_City']}, {record['Property_State']} {record['Property_Zip']}"
            
            response = location_client.search_place_index_for_text(
                IndexName='AddressVerification',
                Text=full_address,
                MaxResults=1
            )
            
            print(response)

            standardized_address = response['Results'][0]['Place']['Label']
            lat = response['Results'][0]['Place']['Geometry']['Point'][1]
            lon = response['Results'][0]['Place']['Geometry']['Point'][0]

            
            yield (record['account_number'], standardized_address, lat, lon)
        except Exception as e:
            yield (record['account_number'], None, None, None)


standardized_addresses = df.rdd.mapPartitions(standardize_addresses_in_partition).toDF(schema=address_schema)

dynamic_frame_to_write = DynamicFrame.fromDF(standardized_addresses, glueContext, "final_frame")

glueContext.write_dynamic_frame.from_options(
   frame=dynamic_frame_to_write, 
   connection_type="s3", 
   connection_options={"path": "s3://attestrds/mstloan/etl/"}, 
   format="csv"
)

job.commit()
