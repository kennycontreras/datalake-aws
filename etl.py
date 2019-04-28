import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window


# Read config file for aws credentials
config = configparser.ConfigParser()
config.read('aws/credentials.cfg')
# Environ variables
os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']

# Spark session configuration


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.5") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
        Process song data function: create a dataframe from all song-data files and process
        the information to buil a star-schema.

        Args:
            Spark (object): Spark Session
            input_data (string): S3 bucket path to read data for dataframes
            output_data (string): S3 bucket to write information from dataframes
    '''

    # filepath for song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # spark dataframe for song data
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + "songs.parquet")

    # extract columns to create artists table
    artists_table = df.select('artist_id', "artist_name", F.col(
        "artist_location").alias("location"), F.col("artist_latitude").alias("latitude"),
        F.col("artist_longitude").alias("longitude"))

    # write artists table to parquet files
    artists_table.write.partitionBy("artist_id").parquet(output_data + "artists.parquet")


def process_log_data(spark, input_data, output_data):
    '''
        Process log data function: create a dataframe from all log-data files and process
        the information to complete the star-schema.

        Args:
            Spark (object): Spark Session
            input_data (string): S3 bucket path to read data for dataframes
            output_data (string): S3 bucket to write information from dataframes
    '''

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table
    users_table = df.select(F.col("userId").alias("user_id"), F.col("firstName").alias(
        "first_name"), F.col("lastName").alias("last_name"), "gender", "level")

    # write users table to parquet files
    users_table.write.partitionBy("user_id").parquet(output_data + "users.parquet")

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
    time_table.write.partitionBy("year", "month").parquet(output_data + "time.parquet")

    # read in song data to use for songplays table
    song_data = input_data + "song_data/*/*/*/*.json"
    dfsong = spark.read.json(song_data)

    # join condition for song and log data
    join_condition = (dfsong.artist_name == df.artist) & (dfsong.title == df.song)

    # Window to create a rownumber column for songplay_id
    window = Window.orderBy(F.monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, join_condition)\
        .select(F.row_number().over(window).alias("songplay_id"),
                df.ts.alias("start_time"), dfLog.userId.alias("user_id"),
                df.level,
                song_df.song_id,
                song_df.artist_id,
                df.sessionId.alias("session_id"),
                df.location,
                df.userAgent.alias("user_agent"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays_table.parquet(output_data + "songplays.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://bucket-etl/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
