from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import random
from flask import request

# Predefined list of valid origins and destinations
valid_origins = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Dubai"]
valid_destinations = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Dubai"]

def analyze_flight_routes():
    # Get user input for origin and destination from the form
    origin = request.form.get('origin')
    destination = request.form.get('destination')

    # Check if the user-provided origin and destination are valid
    if origin not in valid_origins or destination not in valid_destinations:
        return "Invalid origin or destination. Please select from the predefined options."

    # Simulate available flights count randomly
    flight_count = random.randint(0, 100)  # Generate a random count of flights

    # Initialize PySpark session
    spark = SparkSession.builder.appName("Flight Route Analysis").getOrCreate()

    # Load dataset (assuming your dataset has other columns like ArrDelay for processing)
    df = spark.read.csv("airline.csv.shuffle", header=True, inferSchema=True)

    # Preprocessing Step: Replace missing values in 'ArrDelay' with random numbers between 0 and 15
    df_cleaned = df.withColumn(
        "ArrDelay", 
        when(col("ArrDelay").isNull(), random.randint(0, 15)).otherwise(col("ArrDelay"))
    )

    # Cast 'ArrDelay' column to a numeric type (e.g., float)
    df_cleaned = df_cleaned.withColumn("ArrDelay", col("ArrDelay").cast("float"))

    # Filter data by origin and destination
    route_data = df_cleaned.filter((df_cleaned['Origin'] == origin) & (df_cleaned['Dest'] == destination))

    # Perform analysis to compute average delay if flights exist
    avg_delay = None
    if flight_count > 0:
        # Compute average delay (handle empty results)
        avg_delay_data = route_data.groupBy("Origin", "Dest").avg("ArrDelay").collect()

        if len(avg_delay_data) > 0:
            avg_delay = avg_delay_data[0][2]  # Accessing the average delay

    # Stop the Spark session
    spark.stop()

    # Return results as text
    if avg_delay is not None:
        return f"From {origin} to {destination}: Number of flights available: {flight_count}. Average delay: {avg_delay:.2f} minutes."
    else:
        return f"From {origin} to {destination}: Number of flights available: {flight_count}. No average delay data available."