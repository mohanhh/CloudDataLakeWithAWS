import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as sqlFunctions
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import udf


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''Creates Spark session and loads hadoop-aws.jar to work with S3'''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark





def process_song_data(spark, input_data, output_data):
    ''' Process song_data files. 
    Read song_data json files, extract song_id, artist_id, year and duration to create a songs table. Write this table to S3 partitioned by year and artist id 
   Create artist table by extracting artist_id, artist_name, artist_location, artist_latitude, artist_longitude
   Parameters: 
   spark: SparkSession
   input_data: Data folder where to look for song files
   Returns: 
       song_table: Data Frame representing song data frame
       artist_table: Data Frame representing artist table.      
'''
    # get filepath to song data file
    '''reading from S3, as files are under key like song_data/A/B/C/Tr123.json, for Spark to read the files specify folder levels'''
    '''the files are nested under.''' 
    
    song_data = input_data + "song_data/*/*/*/"
    song_frame = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = song_frame.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.coalesce(5).write.mode("OVERWRITE").partitionBy("year", "artist_id").parquet(output_data + "/songs/")

    # extract columns to create artists table
    artists_table = song_frame.selectExpr("artist_id",  "artist_name as name", "artist_location as location", "artist_latitude as latitude", 
                                          "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.coalesce(5).write.mode("OVERWRITE").parquet(output_data + "/artists/")
    return (songs_table,artists_table)



def get_timestamp_udf(ts):
    '''Convert java style time (milli seconds since 1970) to time in seconds. java style to timestamp style'''
    return(ts/1000)




def process_log_data(spark, input_data, output_data, song_df, artist_df):
    '''Read log_data json files. process_song_data has to be called before this function is called. process_song_data processes song and artist 
   files first creates song_df and artist_df frames.
   Filter only those records which have NextSong has action and create time and user dimension tables first.
   As the log file has only song title, song duration compare these values to the corrsponding entries in song table to pick up song_id
   Parameters: 
   spark: SparkSession
   input_data: Data folder where to look for log files
   song_df : Data Frame representing song data
   artist_df : Data Frame representing artist data
'''
    # get filepath to log data file
    log_data = input_data + "log-data/*"
    

    # read log data file
    df = spark.read.json(log_data)
    
    
    # filter by actions for song plays. Filter by NextSong and remove registration or other actions.
    df = df.filter("page == 'NextSong'")

    # extract columns for users table  
   
    users_table = df.selectExpr("userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level", "ts")
    '''Need to select unique user records. As data is being read from log files, there will be duplicate user records.
     create users table by using row_number. As we are setting row_number() ordered by ts in descending order, latest user record will have an id 
     of 1'''
    users_table.createOrReplaceTempView("users_table")
    users = spark.sql("select user_id, first_name, last_name, gender, level, row_number() over (partition by user_id order by ts desc) as ts_order\
                      from users_table")
    users.createOrReplaceTempView("users")
    users_table = spark.sql("select u1.user_id, u1.first_name, u1.last_name, u1.gender, u1.level from users u1 where ts_order = 1")
    users_table = users_table.dropDuplicates(subset=['user_id'])
    
   
    ''' create timestamp column from original timestamp column '''
    get_timestamp = udf(get_timestamp_udf, DoubleType())
    df = df.withColumn("ts_ms", get_timestamp(col("ts").cast(LongType())))    
    df = df.selectExpr("*", "cast(ts_ms as timestamp) timestamp")
    
    ''' Convert timestamp column to month, week, hour, weekofyear, dayofweek, year to fill up our time dimensions ''' 
    
    df = df.withColumn("hour", hour(col("timestamp")))\
    .withColumn("month", month(col("timestamp")))\
    .withColumn("week", weekofyear("timestamp"))\
    .withColumn("weekday", dayofweek("timestamp")).withColumn("year", year(col("timestamp")))\
    .withColumn("new_date", date_format("timestamp", "dd/MM/yyyy"))

    
    # extract columns to create time table
    time_table = df.selectExpr("ts as start_time", "hour", "weekday", "week", "month", "year")
    
    
    ''' read in song data to use for songplays table
     songplays data frame passed as parameter to process_log message
     extract columns from joined song and log datasets to create songplays table. As log file does not have song_id or artist_id, compare
     song title and song length to corresponding values in song table. Similarly compare artist name to corresponding entry in artist table and pick      up artist_id 
     '''
    song_df.createOrReplaceTempView("songs")
    artist_df.createOrReplaceTempView("artist")
    df.createOrReplaceTempView("songs_log")
    time_table.createOrReplaceTempView("time_table")
    songplays_table = spark.sql("select (monotonically_increasing_id() + 1) as song_play_id, songs_log.ts as start_time,\
    time.month,time.year,songs_log.userId, songs_log.level, songs.song_id, artist.artist_id, songs_log.sessionId as session_id, songs_log.location,\
    songs_log.userAgent as user_agent from songs_log songs_log join songs on songs_log.song = songs.title join artist artist on songs_log.artist =\
    artist.name join time_table as time on time.start_time=songs_log.ts")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("OVERWRITE").partitionBy("year", "month").parquet(output_data + "/time/")

     # write users table to parquet files
    users_table.coalesce(1).write.mode("OVERWRITE").parquet(output_data + "/users/")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("OVERWRITE").partitionBy('year', 'month').parquet(output_data + "/songplays/")
    


def main():
    spark = create_spark_session()
    input_data = "data/"
    output_data = "s3a://sparkify-warehouse/"
    
    song_df, artist_df = process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data, song_df, artist_df)


if __name__ == "__main__":
    main()
