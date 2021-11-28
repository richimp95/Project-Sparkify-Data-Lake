import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg', encoding='utf-8-sig')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    
    """
    Description: This function creates the Spark Session that will be used to process all the Sparkify data. 
    Arguments:
        None
    Returns:
        Spark Session
    """
    
    spark = SparkSession.builder\
                     .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                     .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
        
    """
    Description: This function uses the Spark Session previously created to ingest the song_data information, 
    create the tables for songs and artists, and finally save these tables as parquet files in S3. 
    Arguments:
        spark: Spark Session.
        input_data: S3 data path for the json files.
        output_data: S3 data path for the parquet files.
    Returns:
        None
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    #print(song_data)
    
    # read song data file
    df = spark.read.json(song_data)
    #df.printSchema()
    
    df.createOrReplaceTempView("temp_song_data")
    
    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(path=output_data + 'songs_table')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct() 
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
        
    """
    Description: This function uses the Spark Session previously created to ingest the song_data information, 
    create the tables for users, time and songplays, and finally save these tables as parquet files in S3.  
    Arguments:
        spark: Spark Session.
        input_data: S3 data path for the json files.
        output_data: S3 data path for the parquet files.
    Returns:
        None
    """
    
    # get filepath to log data file
    log_data = str(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')
    df.printSchema()
    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users_table')
    
    # extract columns to create time table
    df = df.withColumn('start_time', (df.ts/1000).cast('timestamp'))
    df = df.withColumn('weekday', date_format(df.start_time, 'E'))
    df = df.withColumn('year', year(df.start_time))
    df = df.withColumn('month', month(df.start_time))
    df = df.withColumn('week', weekofyear(df.start_time))
    df = df.withColumn('day', dayofmonth(df.start_time))
    df = df.withColumn('hour', hour(df.start_time))
    
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy('year', 'month').parquet(path=output_data + 'time_table')

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM temp_song_data")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, (df.artist == song_df.artist_name) & (df.song == song_df.title), "inner").distinct()\
                        .select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', df.year, df.month)\
                        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(path=output_data + 'songplays_table')


def main():
        
    """
    Description: This is the main function where everything is executed. 
    Arguments:
        None
    Returns:
        None
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dl-rmp-project/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
