import logging
import uuid
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType

def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
        """)
        logging.info("Keyspace created successfully!")
    except Exception as e:
        logging.error(f"Could not create keyspace due to: {e}")

def create_table(session):
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.forex_table (
            id UUID PRIMARY KEY,
            ticker TEXT,
            bid FLOAT,
            ask FLOAT,
            open_price FLOAT, 
            low FLOAT,
            high FLOAT,
            changes FLOAT,
            date TIMESTAMP
        );
        """)
        logging.info("Table created successfully!")
    except Exception as e:
        logging.error(f"Could not create table due to: {e}")

def insert_data(session, **kwargs):
    logging.info("Inserting data...")
    ticker_id = uuid.uuid4()  # Generates a new UUID
    ticker_name = kwargs.get('ticker')
    bid = kwargs.get('bid')
    ask = kwargs.get('ask')
    open_price = kwargs.get('open')
    low = kwargs.get('low')
    high = kwargs.get('high')
    changes = kwargs.get('changes')
    date = kwargs.get('date')

    try:
        session.execute("""
            INSERT INTO spark_streams.forex_table (id, ticker, bid, ask, open_price, low, high, changes, date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (ticker_id, ticker_name, bid, ask, open_price, low, high, changes, date))
        logging.info(f"Data inserted for {ticker_name} with bid {bid}")
    except Exception as e:
        logging.error(f"Could not insert data due to: {e}")

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2,"
                                           "org.apache.kafka:kafka-clients:3.5.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the Spark session due to exception {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        logging.info("Attempting to create Kafka DataFrame...")
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'forex_data') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka DataFrame created successfully")
        return spark_df
    except Exception as e:
        logging.error(f"Failed to create Kafka DataFrame: {e}")
        return None

def create_cassandra_connection():
    try:
        # Connecting to the Cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        logging.info("Cassandra connection created successfully!")
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None

def create_selection_df_from_kafka(spark_df):
    # Define schema for parsing Kafka messages
    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("bid", FloatType(), True),
        StructField("ask", FloatType(), True),
        StructField("open_price", FloatType(), True),
        StructField("low", FloatType(), True),
        StructField("high", FloatType(), True),
        StructField("changes", FloatType(), True),
        StructField("date", TimestampType(), True),
        # Use StringType for UUID, to be converted in Cassandra
        StructField("id", StringType(), False)
    ])

    try:
        # Cast Kafka value to string and apply schema
        sel = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')) \
            .select("data.*")

        # Add the UUID column as the primary key
        sel = sel.withColumn("id", expr("uuid()"))

        logging.info("Schema successfully applied to Kafka stream.")
        return sel
    except Exception as e:
        logging.error(f"Error applying schema to Kafka stream: {e}")
        return None

if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(level=logging.INFO)

    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka and get data stream
        spark_df = connect_to_kafka(spark_conn)
        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)
            session = create_cassandra_connection()

            if session:
                create_keyspace(session)
                create_table(session)

                logging.info("Starting the streaming process...")

                # Write data to Cassandra
                streaming_query = (selection_df.writeStream
                                   .format("org.apache.spark.sql.cassandra")
                                   .option('checkpointLocation', '/tmp/checkpoint')
                                   .option('keyspace', 'spark_streams')
                                   .option('table', 'forex_table')
                                   .start())

                # Await termination
                streaming_query.awaitTermination()
