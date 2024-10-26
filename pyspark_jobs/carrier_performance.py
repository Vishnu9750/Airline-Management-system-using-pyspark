from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from flask import request

def carrier_performance_route():
    # Get input from user (UniqueCarrier from the form submission)
    input_carrier = request.form.get('UniqueCarrier')

    if not input_carrier:
        return "Please provide a carrier code."

    # Initialize PySpark session
    spark = SparkSession.builder.appName("Carrier Performance Comparison").getOrCreate()

    try:
        # Load dataset
        df = spark.read.csv("airline.csv.shuffle", header=True, inferSchema=True)

        # Filter data for the provided carrier
        carrier_df = df.filter(df['UniqueCarrier'] == input_carrier)

        # Check if the carrier exists in the data
        if carrier_df.count() == 0:
            return f"No data found for carrier: {input_carrier}"

        # Convert ArrDelay to numeric, coercing errors to null
        carrier_df = carrier_df.withColumn("ArrDelay", col("ArrDelay").cast("double"))

        # Drop rows with null values in ArrDelay
        carrier_df = carrier_df.na.drop(subset=["ArrDelay"])

        # Analyze performance (e.g., average delay, on-time percentage)
        avg_delay = carrier_df.groupBy("UniqueCarrier").agg({"ArrDelay": "avg"}).collect()[0][1]
        on_time_count = carrier_df.filter(carrier_df["ArrDelay"] <= 0).count()
        total_flights = carrier_df.count()
        on_time_percentage = (on_time_count / total_flights) * 100 if total_flights > 0 else 0

        # Return results as a string
        return f"Carrier {input_carrier} has an average delay of {avg_delay:.2f} minutes and an on-time performance of {on_time_percentage:.2f}%."

    except Exception as e:
        return f"An error occurred: {str(e)}"

    finally:
        # Ensure the Spark session is stopped
        spark.stop()
