
#Loading the modules 
import configparser
from datetime import datetime
import os
import pyspark.sql.functions as transform
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')


# Creating spark session 
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """Extracting song data from json files stored in S3 and transforming them to the formats we need and writing them back into S3 tables
    
    Arguments:
            spark :     Spark session to facilitate the extraction and transformations
            input_data(string) :   song data to be extracted from S3 
           output_data(string):  S3 bucket to store the transformed files as parquets in S3
      
    """
    
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs/'+'songs.parquet', partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).distinct()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists/' + 'artists.parquet', partitionBy=['artist_id'] )


def process_log_data(spark, input_data, output_data):
    """This function in for transforming and loading the data for analytical purposes
    
    Arguments:
            spark :     Spark session to facilitate the extraction and transformations
            input_data(string) :   song data to be extracted from S3 
           output_data(string):  S3 bucket to store the transformed files as parquets in S3
   
    """
        
    # get filepath to log data file
    # log_data = log_data = input_data + "log_data/*/*/"
    log_data = log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    log_df = spark.read.json(log_data)
    
    # filter by actions for song plays
    log_df =log_df.where('page="NextSong"')

    # extract columns for users table    
    users_table = log_df.select(["userId", "firstName", "lastName", "gender", "level"]).distinct()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + 'users/' + 'users.parquet', partitionBy = ['userId'])

    # create timestamp column from original timestamp column
    log_df = log_df.withColumn('timestamp',( (log_df.ts.cast('float')/1000).cast("timestamp")) )
    
    # extract columns to create time table
    time_table = log_df.select(
                    transform.col("timestamp").alias("start_time"),
                    transform.hour("timestamp").alias('hour'),
                    transform.dayofmonth("timestamp").alias('day'),
                    transform.weekofyear("timestamp").alias('week'),
                    transform.month("timestamp").alias('month'), 
                    transform.year("timestamp").alias('year'), 
                    transform.date_format(transform.col("timestamp"), "E").alias("weekday")
                )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + 'time/' + 'time.parquet', partitionBy=['start_time'])

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data+'song_data/*/*/*/*.json')
    
    # join song_df and log_df
    song_log_joined_table = log_df.join(song_df, (log_df.song == song_df.title) & (log_df.artist == song_df.artist_name) & (log_df.length == song_df.duration), how='inner')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_log_joined_table.distinct() \
                        .select("userId", "timestamp", "song_id", "artist_id", "level", "sessionId", "location", "userAgent" ) \
                        .withColumn("songplay_id", F.row_number().over( Window.partitionBy('timestamp').orderBy("timestamp"))) \
                        .withColumnRenamed("userId","user_id")        \
                        .withColumnRenamed("timestamp","start_time")  \
                        .withColumnRenamed("sessionId","session_id")  \
                        .withColumnRenamed("userAgent", "user_agent") \

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + 'songplays/' + 'songplays.parquet',partitionBy=['start_time', 'user_id'])



def main():
      
    """
    Calling the functions to transform data
        
    """
    # Loading AWS credentials
    config = configparser.ConfigParser()
    config.read_file(open('dl.cfg'))

    os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']
    
    # setting up spark and input and output paths
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify_datalake/"
    
    # running the functions to achieve the transformations 
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
