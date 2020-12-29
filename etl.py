import configparser
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """

    Args:
        spark: spark session
        input_data: storage root path
        output_data: storage output root path
    """
    song_data = input_data + "song_data/*/*/*"

    raw_song_data_schema = T.StructType([
        T.StructField("artist_id", T.StringType(), True),
        T.StructField("artist_latitude", T.DoubleType(), True),
        T.StructField("artist_location", T.StringType(), True),
        T.StructField("artist_longitude", T.DoubleType(), True),
        T.StructField("artist_name", T.StringType(), True),
        T.StructField("duration", T.DoubleType(), True),
        T.StructField("num_songs", T.LongType(), True),
        T.StructField("song_id", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("year", T.LongType(), True),
    ])

    # read song data file
    raw_song_data = spark.read.schema(raw_song_data_schema).json(song_data)
    raw_song_data.createOrReplaceTempView("song_data")

    # extract columns to create songs table
    songs_query = '''
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM song_data
    GROUP BY song_id, title, artist_id, year, duration
    '''

    songs = spark.sql(songs_query)

    # write songs table to parquet files partitioned by year and artist
    output_path = os.path.join(output_data, "songs")
    songs.write.partitionBy("year", "artist_id").parquet(output_path, "overwrite")

    # extract columns to create artists table
    artists_query = '''
    SELECT DISTINCT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude 
    FROM song_data sd1
    WHERE year = (SELECT MAX(year) FROM song_data sd2 WHERE sd1.artist_id = sd2.artist_id)
    AND duration = (SELECT MAX(duration) FROM song_data sd2 WHERE sd1.artist_id = sd2.artist_id AND sd1.year = sd2.year)
    GROUP BY artist_id, name, location, latitude, longitude'''

    artists = spark.sql(artists_query)
    
    # write artists table to parquet files
    output_path = os.path.join(output_data, "artists")
    artists.write.parquet(output_path, "overwrite")


def process_log_data(spark, input_data, output_data):
    """process log files alongside the songs data to generate user and time dimensions and songplays facts

    Args:
        spark: spark session
        input_data: storage root path
        output_data: storage output root path
    """

    log_data = input_data + "log_data/**/*"

    raw_event_data_schema = T.StructType([
        T.StructField("artist", T.StringType(), True),
        T.StructField("auth", T.StringType(), True),
        T.StructField("firstName", T.StringType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("itemInSession", T.LongType(), True),
        T.StructField("lastName", T.StringType(), True),
        T.StructField("length", T.DoubleType(), True),
        T.StructField("level", T.StringType(), True),
        T.StructField("location", T.StringType(), True),
        T.StructField("method", T.StringType(), True),
        T.StructField("page", T.StringType(), True),
        T.StructField("registration", T.DoubleType(), True),
        T.StructField("sessionId", T.LongType(), True),
        T.StructField("song", T.StringType(), True),
        T.StructField("status", T.IntegerType(), True),
        T.StructField("ts", T.LongType(), True),
        T.StructField("userAgent", T.StringType(), True),
        T.StructField("userId", T.StringType(), True),
    ])

    # read log data file
    raw_event_data = spark.read.schema(raw_event_data_schema).json(log_data)

    clean_event_data = raw_event_data.withColumn("ts", F.to_timestamp(F.from_unixtime(F.col("ts") / 1000))).withColumn(
        "userId", raw_event_data.userId.cast(T.IntegerType()))

    clean_event_data.createOrReplaceTempView("event_data")

    # extract columns for users table    
    users_query = '''
    SELECT DISTINCT userId AS user_id, firstName AS first_name, lastName AS last_name, gender, level
    FROM event_data ed1
    WHERE userid IS NOT NULL
    AND page = 'NextSong'
    AND ts = (SELECT max(ts) FROM event_data ed2 WHERE page = 'NextSong' AND ed2.userId = ed1.userId)
    GROUP BY userId, firstName, lastName, gender, level
    '''

    users = spark.sql(users_query)
    
    # write users table to parquet files
    output_path = os.path.join(output_data, "users")
    users.write.parquet(output_path, "overwrite")

    # extract columns to create time table
    times_query = '''
    SELECT DISTINCT ts AS start_time,
    EXTRACT(hour FROM ts) AS hour,
    EXTRACT(day FROM ts) AS day,
    EXTRACT(week FROM ts) AS week,
    EXTRACT(month FROM ts) AS month,
    EXTRACT(year FROM ts) AS year,
    dayofweek(ts) AS weekday
    FROM event_data
    WHERE page = 'NextSong'
    GROUP BY ts
    '''

    times = spark.sql(times_query)

    # write time table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, "users")
    times.write.partitionBy("year", "month").parquet(output_path, "overwrite")

    # read in song data to use for songplays table
    raw_song_data_schema = T.StructType([
        T.StructField("song_id", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("artist_id", T.StringType(), True),
        T.StructField("year", T.LongType(), True),
        T.StructField("duration", T.DoubleType(), True),
    ])
    songs_path = os.path.join(output_data, "songs")
    songs = spark.read.schema(raw_song_data_schema).parquet(songs_path)

    # extract columns from joined song and log datasets to create songplays table
    songs.createOrReplaceTempView("songs")

    song_plays_query = """
    SELECT se1.ts AS songplay_timestamp, se1.userid AS user_id, se1.level AS user_level, s.song_id AS song_id, s.artist_id AS artist_id, se1.sessionid AS session_id, se1.location AS location, se1.useragent AS user_agent
    FROM event_data se1
    JOIN songs s ON s.title = se1.song AND s.duration = se1.length
    WHERE userId IS NOT NULL
    AND page = 'NextSong'"""

    song_plays = spark.sql(song_plays_query)

    times.createOrReplaceTempView("times")
    song_plays.createOrReplaceTempView("song_plays")
    songplays_with_time_query = '''
    SELECT songplay_timestamp, user_id, user_level, song_id, artist_id, session_id, location, user_agent, t.month AS month, t.year AS year
    FROM song_plays sp
    JOIN times t ON t.start_time = sp.songplay_timestamp
    '''
    songplays_with_time = spark.sql(songplays_with_time_query)

    # write songplays table to parquet files partitioned by year and month
    output_path = os.path.join(output_data, "songplays")
    songplays_with_time.write.partitionBy("year", "month").parquet(output_path, "overwrite")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
