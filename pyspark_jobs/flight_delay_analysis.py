from pyspark.sql import SparkSession
from flask import request

def analyze_flight_delay(FlightNum):
    # Initialize PySpark session
    spark = SparkSession.builder.appName("Flight Delay Analysis").getOrCreate()

    # Load dataset
    df = spark.read.csv("airline.csv.shuffle", header=True, inferSchema=True)

    # Filter data for the provided flight ID
    delay_data = df.filter(df['FlightNum'] == FlightNum)

    # Perform analysis on delay times
    avg_delay = delay_data.groupBy("FlightNum").agg({"ArrDelay": "avg"}).collect()[0][1]

    spark.stop()

    # Return results as text
    return f"Average delay time for Flight {FlightNum} is {avg_delay:.2f} minutes."
