from pyspark.sql import SparkSession
from flask import request

def analyze_customer_satisfaction():
    # Get user input (FlightNum for specific flight analysis)
    FlightNum = request.form.get('FlightNum')

    # Initialize PySpark session
    spark = SparkSession.builder.appName("Customer Satisfaction Analysis").getOrCreate()

    # Load dataset
    df = spark.read.csv("airline.csv.shuffle", header=True, inferSchema=True)

    # Remove rows with missing values in relevant columns (ArrDelay and FlightNum)
    clean_df = df.dropna(subset=['ArrDelay', 'FlightNum'])

    # Filter data by flight number
    flight_data = clean_df.filter(clean_df['FlightNum'] == FlightNum)

    # Analyze on-time performance
    total_flights = flight_data.count()
    on_time_flights = flight_data.filter(flight_data['ArrDelay'] <= 0).count()

    if total_flights > 0:
        on_time_percentage = (on_time_flights / total_flights) * 100
    else:
        on_time_percentage = 0

    spark.stop()

    # Return results as text
    return f"On-time performance for Flight {FlightNum}: {on_time_percentage:.2f}% flights arrived on time or earlier."
