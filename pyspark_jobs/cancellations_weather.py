from pyspark.sql import SparkSession
from flask import request

def analyze_cancellations_weather(FlightNum):
    # Initialize PySpark session
    spark = SparkSession.builder.appName("Cancellations and Weather Impact").getOrCreate()

    # Load dataset and remove missing values
    df = spark.read.csv("airline.csv.shuffle", header=True, inferSchema=True).na.drop()

    # Filter data for the provided FlightNum
    cancellation_data = df.filter(df['FlightNum'] == FlightNum)

    # Perform analysis on cancellations and weather
    cancelled_flights = cancellation_data.filter(cancellation_data['Cancelled'] == 1).count()
    weather_impact = cancellation_data.filter(cancellation_data['CancellationCode'] == 'W').count()

    spark.stop()

    # Return results as text
    return f"Flight {FlightNum} had {cancelled_flights} cancellations, of which {weather_impact} were due to weather."
