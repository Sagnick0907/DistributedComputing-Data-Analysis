from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import time

try:
    # Start the timer
    start_time = time.time()

    # Create a Spark session
    mySpark = SparkSession.builder.appName("HousePricingAnalysis").getOrCreate()

    # Create an SQL context
    sqlContext = SQLContext(mySpark)

    # Read the CSV file into a DataFrame
    data = mySpark.read.csv("House_Pricing.csv", header=True, inferSchema=True)

    # Select the required columns
    required_data = data.select("Transaction unique identifier", "Price", "Country")

    # Register the DataFrame as a temporary table
    required_data.createOrReplaceTempView("house_pricing")

    # Use mySpark.sql to perform the analysis and save the result to a CSV file
    finalResult = mySpark.sql("SELECT Country, COUNT(*) AS Count FROM house_pricing GROUP BY Country")

    # Save the result to a CSV file, overwriting if it already exists
    finalResult.coalesce(1).write.mode("overwrite").csv("2023201008_q3/transaction_count_per_country", header=True)

     # Stop the timer
    end_time = time.time()

    # Calculate and display the execution time
    execution_time = end_time - start_time
    print(f"Execution time: {execution_time} seconds.")
    
except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Stop the Spark session in the 'finally' block to ensure proper cleanup
    if 'mySpark' in locals():
        mySpark.stop()
