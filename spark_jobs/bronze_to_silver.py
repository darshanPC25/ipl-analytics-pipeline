import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

load_dotenv()

# Config
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BRONZE_BUCKET = os.getenv("S3_BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("S3_SILVER_BUCKET")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def create_spark_session():
    """Create Spark session with S3 and Postgres support"""
    spark = SparkSession.builder \
        .appName("IPL Bronze to Silver") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark Session created successfully!")
    return spark

def read_bronze(spark):
    """Read raw data from S3 Bronze"""
    print("Reading data from S3 Bronze...")
    bronze_path = f"s3a://{BRONZE_BUCKET}/ipl/IPL/*/*/*/*"
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(bronze_path)
    
    print(f"Total records in Bronze: {df.count()}")
    print("Schema:")
    df.printSchema()
    return df

def clean_data(df):
    """Clean and transform data - Silver layer"""
    print("Cleaning Data...")
    
    # Replace empty strings with nulls
    for col in df.columns:
        df = df.withColumn(
            col,
            F.when(F.col(col) == "", None).otherwise(F.col(col))
        )
    
    # Remove duplicate rows
    before = df.count()
    df = df.dropDuplicates()
    after = df.count()
    print(f"Removed {before - after} duplicate records")

    # Drop rows where critical columns are null 
    df = df.dropna(subset=["match_id", "season", "date"])

    # Standardize column names to lowercase
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower().replace(" ", "_"))

    print(f"Total records after cleaning: {df.count()}")
    return df

def write_to_silver_s3(df):
    """Write cleaned data to S3 Silver"""
    print("Writing cleaned data to S3 silver...")

    silver_path = f"s3a://{SILVER_BUCKET}/ipl/matches/"

    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(silver_path)

    print("Written to S3 Silver Successfully!")


def write_to_postgres(df):
    """Write cleaned data to Postgres"""
    print("Writing cleaned data to Postgres...")

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", "silver.ipl_matches") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    print("Written to Postgres Successfully!")


def main():
    print("=" * 50)
    print("IPL Analytics Pipeline - Bronze to Silver")
    print("=" * 50)

    # Create Spark session
    spark = create_spark_session()

    # Read from Bronze
    df = read_bronze(spark)

    # Clean data
    df_clean = clean_data(df)

    # Write to Silver S3
    write_to_silver_s3(df_clean)

    # Write to PostgreSQL
    write_to_postgres(df_clean)

    print("=" * 50)
    print("Bronze to Silver Transformation Complete!")
    print("=" * 50)

    spark.stop()

if __name__ == "__main__":
    main()
    


        
