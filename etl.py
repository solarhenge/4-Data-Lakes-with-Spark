import argparse
import sys
argument_count = 2

if len(sys.argv) < argument_count:
    print('expecting '+str(argument_count-1)+' argument(s)')
    print('i.e.')
    print('To Parquet...')
    print('python etl.py true')
    print('or Not To Parquet...')
    print('python etl.py false')
    sys.exit()

#
# str2bool
# purpose: to convert a string and return a boolean
#
def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')    

parquet = str2bool(sys.argv[1])

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofyear, minute, second, unix_timestamp, dayofweek

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime

import platform

config = configparser.ConfigParser()

config_file_name = 'dl.cfg'
if platform.system() == 'Windows':
    config_file_name = 'D:/AWS/'+config_file_name

config.read(config_file_name)

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWSAccessKeyId']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWSSecretKey']

#
# create_spark_session
# purpose: to create a spark session
#
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.sql.broadcastTimeout", "36000") \
        .appName("Project: Data Lake") \
        .getOrCreate()
    return spark

#
# process_song_dataset
# purpose: to process the song dataset
# return(s): song and artist dataframes to be used in process_log_dataset
#
def process_song_dataset(spark, song_dataset, output_data):
    # read song data file
    df_staging_songs = spark.read.json(song_dataset)

    # extract columns to create songs table
    df_songs = df_staging_songs.select(df_staging_songs.year.alias('year')
                                      ,df_staging_songs.artist_id.alias('artist_id')
                                      ,df_staging_songs.song_id.alias('song_id')
                                      ,df_staging_songs.title.alias('title')
                                      ,df_staging_songs.duration.alias('duration')
                                      )

    # drop duplicate song rows
    df_songs = df_songs.drop_duplicates(subset=['year','artist_id','song_id'])

    # extract columns to create artists table
    df_artists = df_staging_songs.select(df_staging_songs.artist_id.alias('artist_id')
                                        ,df_staging_songs.artist_name.alias('name')
                                        ,df_staging_songs.artist_location.alias('location')
                                        ,df_staging_songs.artist_latitude.alias('latitude')
                                        ,df_staging_songs.artist_longitude.alias('longitude')
                                        )
    # blank_as_null
    # purpose: to change a column that contains an empty string to a null value
    def blank_as_null(x):
        return when(col(x) != "", col(x)).otherwise(None)

    # change artists location from empty string to null value
    df_artists = df_artists.withColumn("location", blank_as_null("location"))

    # drop duplicate artist rows
    #df_artists = df_artists.drop_duplicates(subset=['artist_id','name'])
    df_artists = df_artists.drop_duplicates(subset=['artist_id'])
    
    return df_songs, df_artists

#
# process_log_dataset
# purpose: to process the log dataset
#
def process_log_dataset(spark, log_dataset, output_data, df_songs, df_artists, parquet, include_the_dimensions):
    # read log data file
    df_staging_events = spark.read.json(log_dataset) 

    # create the get timestamp user defined function
    get_timestamp_udf = F.udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 

    # filter events i.e. page = NextSong
    df_staging_events = df_staging_events.where(col('page').isin({'NextSong'}))

    # convert the ts column from epoch to timestamp
    df_staging_events = df_staging_events.withColumn("start_time", get_timestamp_udf(df_staging_events.ts))
    df_staging_events = df_staging_events.withColumn("year", year(df_staging_events.start_time))
    df_staging_events = df_staging_events.withColumn("month", month(df_staging_events.start_time))
    
    # create time dataframe and drop duplicate ts rows
    df_time = df_staging_events.drop_duplicates(subset=['ts'])

    # convert the ts column from epoch to timestamp
    df_time = df_time.withColumn("start_time", get_timestamp_udf(df_time.ts))

    # create time dimension dataframe
    df_time = df_time.select(df_time.start_time.alias('start_time')
                            ,year(df_time.start_time).alias('year')
                            ,month(df_time.start_time).alias('month')
                            ,dayofmonth(df_time.start_time).alias('dayofmonth')
                            ,hour(df_time.start_time).alias('hour')
                            ,minute(df_time.start_time).alias('minute')
                            ,second(df_time.start_time).alias('second')
                            ,dayofweek(df_time.start_time).alias('dayofweek')
                            ,dayofyear(df_time.start_time).alias('dayofyear')
                            ,weekofyear(df_time.start_time).alias('weekofyear')
                            )

    # create users dimension
    df_users = df_staging_events.select(df_staging_events.userId.alias('user_id')
                                       ,df_staging_events.firstName.alias('first_name')
                                       ,df_staging_events.lastName.alias('last_name')
                                       ,df_staging_events.gender.alias('gender')
                                       )

    # drop duplicate user rows
    df_users = df_users.drop_duplicates(subset=['user_id'])

    # create temporary views
    df_staging_events.createOrReplaceTempView("staging_events")
    df_artists.createOrReplaceTempView("artists")
    df_songs.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    df_songplays = spark.sql("""select null as songplay_id ,se.start_time ,se.year ,se.month ,se.userId as user_id ,se.level ,s.song_id ,a.artist_id ,se.sessionId as session_id ,se.location ,se.userAgent as user_agent from staging_events se join artists a on se.artist = a.name join songs s on se.song = s.title and s.artist_id = a.artist_id where 1 = 1""")
    # populate songplays surrogate key
    df_songplays = df_songplays.withColumn("songplay_id", monotonically_increasing_id())

    if parquet:
        # write songplays table to parquet files partitioned by year and month
        df_songplays.write.partitionBy('year', 'month').parquet(output_data + "songplays", mode="overwrite")

    if include_the_dimensions:
        df_songplays.createOrReplaceTempView("songplays")
        df_artists = spark.sql("""select distinct a.* from artists a join songplays sp on a.artist_id = sp.artist_id where 1 = 1""")
        df_songs = spark.sql("""select distinct s.* from songs s join songplays sp on s.song_id = sp.song_id where 1 = 1""")
        df_time.createOrReplaceTempView("time")
        df_time = spark.sql("""select distinct t.* from time t join songplays sp on t.start_time = sp.start_time where 1 = 1""")
        df_users.createOrReplaceTempView("users")
        df_users = spark.sql("""select distinct u.* from users u join songplays sp on u.user_id = sp.user_id where 1 = 1""")

    if parquet:
        if include_the_dimensions:
            # write users table to parquet files partitioned by none
            df_users.write.parquet(output_data + "users", mode="overwrite")

            # write songs table to parquet files partitioned by year and artist
            df_songs.write.partitionBy('year', 'artist_id').parquet(output_data + "songs", mode="overwrite")

            # write artists table to parquet files partitioned by none
            df_artists.write.parquet(output_data + "artists", mode="overwrite")

            # write time table to parquet files partitioned by year and month
            df_time.write.partitionBy('year', 'month').parquet(output_data + "time", mode="overwrite")

    # print table info
    print('songplays info...')
    print('count(s)')
    df_songplays.groupby(df_songplays.year,df_songplays.month).count().orderBy(df_songplays.year,df_songplays.month).show()
    df_songplays.printSchema()
    df_songplays.show(5)

    if include_the_dimensions:
        print('users info...')
        print('count(s)')
        print(df_users.count())
        df_users.printSchema()
        df_users.show(5)

        print('songs info...')
        print('count(s)')
        df_songs.groupby(df_songs.year).count().orderBy(df_songs.year).show()
        df_songs.printSchema()
        df_songs.show(5)

        print('artists info...')
        print('count(s)')
        print(df_artists.count())
        df_artists.printSchema()
        df_artists.show(5)
    
        print('time info...')
        print('count(s)')
        df_time.groupby(df_time.year,df_time.month).count().orderBy(df_time.year,df_time.month).show()
        df_time.printSchema()
        df_time.show(5)

def main():
    spark = create_spark_session()

    if platform.system() == 'Windows':   
        input_data = "s3a://udacity-dend/"
        #input_data = "D:/Data-Lake/input_data/"
        output_data = "D:/Data-Lake/output_data/"
    else:
        input_data = "s3a://udacity-dend/"
        output_data = "s3a://solarhenge/output_data/"

    # get filepath to song data file
    song_dataset = os.path.join(input_data, "song_data/*/*/*/*.json")    

    # get filepath to log data file
    log_dataset = os.path.join(input_data, "log_data/*/*/*.json")

    df_songs, df_artists = process_song_dataset(spark, song_dataset, output_data)    

    parquet = str2bool(sys.argv[1])
    print("parquet = %r" % parquet)

    include_the_dimensions = True
    print("include_the_dimensions = %r" % include_the_dimensions)

    process_log_dataset(spark, log_dataset, output_data, df_songs, df_artists, parquet, include_the_dimensions)

    spark.stop()

if __name__ == "__main__":
    main()