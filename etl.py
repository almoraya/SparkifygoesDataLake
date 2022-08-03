import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, desc
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import TimestampType



config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    - it creates an entry point to all Spark features
    - it provides key-value pairs Spark parameters    
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    - it process the song data as specified below
    - it provide a series of input parameters: 
        - spark session 
        - input buckt 
        - output buket
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    dfsong = spark.read.json(song_data)

    ### I have added the temp_artname column to be able to use the songs table twice when join it to the songplays table    
    # extract columns to create songs table
    songs_table = dfsong.select(["song_id", "title", "artist_id", "year", "duration", "artist_name"]). \
                  withColumnRenamed("artist_name", "temp_artname").filter(dfsong['song_id'].isNotNull()).dropDuplicates()
    
    ## https://sparkbyexamples.com/pyspark/pyspark-read-and-write-parquet-file/
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year', 'artist_id']).mode('overwrite').parquet(output_data + 'songs/songs_table.parquet/')

    # extract columns to create artists table
    artists_table = dfsong.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).filter(dfsong['artist_id'].isNotNull()).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists/artists_table.parquet/')


def process_log_data(spark, input_data, output_data):
    """
    - it process the song data as specified below
    - it provide a series of input parameters: 
        - spark session 
        - input buckt 
        - output buket
    """
    
    # get filepath to log data file
    log_data = input_data + 'log-data/*/*/*.json'

    # read log data file
    dflog = spark.read.json(log_data)
    
    # filter by actions for song plays
    dflog = dflog.filter("page = 'NextSong'")

    # extract columns for users table    
    users_table = dflog.select(["userId", "firstName", "lastName", "gender", "level"]).filter(dflog['userId'].isNotNull()).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data + 'users/users_table.parquet/')

    
    ## from lesson 2: spark.udf.register('get_hour', lambda x: int(datetime.datetime.fromtimestamp(x/1000.0).hour))
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df_time = dflog.select("ts").withColumn("start_time", get_timestamp(dflog['ts']))
    
    
    # create datetime column from original timestamp column (?????)
    df_time = df_time.withColumn("hour", hour("start_time"))
    df_time = df_time.withColumn("day", dayofmonth("start_time"))
    df_time = df_time.withColumn("week", weekofyear("start_time"))
    df_time = df_time.withColumn("month", month("start_time"))
    df_time = df_time.withColumn("year", year("start_time"))
    df_time = df_time.withColumn("weekday", dayofweek("start_time")) 
    
       
    # extract columns to create time table
    time_table = df_time.select(["start_time", "hour", "day", "week", "month", "year", "weekday"]).filter(df_time['start_time'].isNotNull()).dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year', 'month']).mode("overwrite").parquet(output_data + 'time/time_table.parquet/')

    ## https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
    # read in song data to use for songplays table
    song_df = spark.read.option("mergeSchema", "true").parquet(output_data + 'songs/songs_table.parquet/')

        
    ## https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6
    ## https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.monotonically_increasing_id.html    
    # extract columns from joined song and log datasets to create songplays table
    
    songplay_df = dflog.select(["ts", "userId", "level", "song", "artist", "sessionId", "location", "userAgent"]). \
              join(song_df, (dflog.song == song_df.title) &(dflog.artist == song_df.temp_artname)). \
              withColumn("start_time", get_timestamp(dflog['ts'])). \
              withColumn("songplay_id", monotonically_increasing_id())
    
    
    songplays_table = songplay_df.select(["songplay_id", "start_time", "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent"]). \
                  withColumn("month", month("start_time")). \
                  withColumn("year", year("start_time"))
    

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year', 'month']).mode("overwrite").parquet(output_data + 'songplays/songplays_table.parquet', partitionBy=['year', 'month'])


def main():
    """
    - it initalize our spark session
    - it fixes our input and output buckets
    - it runs our process_song_data function
    - it runs our process_log_data function
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-outlake/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
