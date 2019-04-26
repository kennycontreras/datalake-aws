import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('aws/credentials.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .config("fs.s3a.awsAccessKeyId", os.environ["AWS_ACCESS_KEY_ID"]) \
        .config("fs.s3a.awsSecretAccessKey", os.environ["AWS_SECRET_ACCESS_KEY"]) \
        .config("fs.s3a.endpoint", 's3.us-west-2.amazonaws.com') \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark

# Schema for Song DataFrame


def song_schema():
    schema = T.StructType([
        T.StructField("artist_id", T.StringType()),
        T.StructField("artist_latitude", T.DoubleType()),
        T.StructField("artist_longitude", T.DoubleType()),
        T.StructField("artist_location", T.StringType()),
        T.StructField("artist_name", T.StringType()),
        T.StructField("duration", T.DoubleType()),
        T.StructField("num_songs", T.IntegerType()),
        T.StructField("song_id", T.StringType()),
        T.StructField("title", T.StringType()),
        T.StructField("year", T.IntegerType())
    ])
    return schema

# Schema for Log DataFrame


def log_schema():
    schema = T.StructType([
        T.StructField("artist", T.StringType()),
        T.StructField("auth", T.StringType()),
        T.StructField("firstName", T.StringType()),
        T.StructField("gender", T.StringType()),
        T.StructField("itemInSession", T.IntegerType()),
        T.StructField("lastName", T.StringType()),
        T.StructField("length", T.DoubleType()),
        T.StructField("level", T.StringType()),
        T.StructField("location", T.StringType()),
        T.StructField("method", T.StringType()),
        T.StructField("page", T.StringType()),
        T.StructField("registration", T.StringType()),
        T.StructField("sessionId", T.IntegerType()),
        T.StructField("song", T.StringType()),
        T.StructField("status", T.StringType()),
        T.StructField("ts", T.LongType()),
        T.StructField("userAgent", T.StringType()),
        T.StructField("userId", T.StringType())
    ])

    return schema


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + "song_data/*.json"

    # read song data file
    df = spark.read.json(song_data, schema=song_schema())

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs/")

    # extract columns to create artists table
    artists_table = df.select('artist_id', "artist_name", F.col(
        "artist_location").alias("location"), F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude"))

    # write artists table to parquet files
    artists_table.write.partitionBy("artist_id").parquet(output_data + "artists/")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*.json"

    # read log data file
    df = spark.read.json(log_data, schema=log_schema())

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    users_table = df.select(F.col("userId").alias("user_id"), F.col("firstName").alias(
        "first_name"), F.col("lastName").alias("last_name"), "gender", "level")

    # write users table to parquet files
    users_table.write.partitionBy("user_id").parquet(output_data + "users/")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp((x/1000.0)), T.TimestampType())
    df = df.withColumn("ts", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    # get_datetime = udf()
    # df =

    # extract columns to create time table
    time_table = df.select(F.col("ts").alias("start_time"),
                           F.hour("ts").alias("hour"),
                           F.dayofmonth("ts").alias("day"),
                           F.weekofyear("ts").alias("week"),
                           F.month("ts").alias("month"),
                           F.year("ts").alias("year"),
                           F.dayofweek("ts").alias("weekday"))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(output_data + "time/")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*.json"
    song_df = spark.read.json(song_data, schema=song_schema())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (song_df.artist_name == df.artist) & (song_df.title == df.song))\
        .select((F.monotonically_increasing_id()+1).alias("songplay_id"),
                df.ts.alias("start_time"), dfLog.userId.alias("user_id"),
                df.level,
                song_df.song_id,
                song_df.artist_id,
                df.sessionId.alias("session_id"),
                df.location,
                df.userAgent.alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    # songplays_table


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bucket-etl/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
