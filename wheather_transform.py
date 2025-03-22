from prompt_toolkit.contrib.telnet.log import logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
spark = SparkSession.builder.appName("WeatherDataCleaning").getOrCreate()
wheather_df = spark.read.csv("weather_data.csv", header=True, inferSchema=True)
wheather_df.printSchema()
# wheather_df.show(5)

print(wheather_df.count())
# df = df.dropna()

# droping row which has missing or empty
clean_wheather_df=wheather_df.dropna()

clean_wheather_df = clean_wheather_df.withColumnRenamed("temperature", "Temperature (°C)")
clean_wheather_df = clean_wheather_df.withColumnRenamed("humidity", "Humidity (%)")
clean_wheather_df =clean_wheather_df.withColumnRenamed("weather", "Weather Condition")


new_clean_df=clean_wheather_df.fillna({"temperature (°C)": 25, "humidity (%)": 50})

# Dropping duplicates values

clean_df=new_clean_df.dropDuplicates()


clean_df.printSchema()
clean_df.show()
clean_df.write.csv("cleaned_weather_data.csv", header=True, mode="overwrite")


# wheatherdataupload

import boto3

# AWS Configuration
AWS_ACCESS_KEY = "access_key"
AWS_SECRET_KEY = "secret_key"
bucket_name = "bucket_name"
local_path = "/Users/abhishek/Desktop/test/wheather/clean_data.csv"  # Path to your CSV file
s3_file_name = "wheather/data.csv"  # File name in S3 bucket

local_file_path="/Users/abhishek/Desktop/downloaded_cleaned_data.csv"

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Upload file
try:
    s3_client.upload_file(local_path, bucket_name, s3_file_name)
    print(f"✅ File '{local_path}' successfully uploaded to S3 bucket '{bucket_name}' as '{s3_file_name}'")
except Exception as e:
    print(f"❌ Error uploading file: {str(e)}")



# To download the file which is present in our bucket for visualization
try:
    # Download File
    s3_client.download_file(bucket_name,f"{s3_file_name}", local_file_path)
    print(f"✅ File downloaded successfully as {local_file_path}")
except Exception as e:
    print(f"❌ Error downloading file: {e}")