from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
import time

try:
    # Record the start time
    start_time = time.time()

    # Initialize a Spark session
    mySpark = SparkSession.builder.appName("HousePricingCustomAnalysis").getOrCreate()

    # Read the CSV file into an RDD
    csv_file_path = "House_Pricing.csv"
    rdd = mySpark.sparkContext.textFile(csv_file_path)

    # Parse the CSV data and create a DataFrame
    header = rdd.first()
    data_rdd = rdd.filter(lambda row: row != header).map(lambda line: line.split(","))

    # Extract only the specified columns: 'Transaction unique identifier', 'Price', 'Country'
    data_row_rdd = data_rdd.map(lambda p: Row(
        transaction_id=str(p[0]),
        price=float(p[1]),  
        country=p[8]
    ))

    # Create a DataFrame
    df = mySpark.createDataFrame(data_row_rdd)

    # Use PySpark DataFrame operations to find the second-highest property sold in selected countries
    selected_countries = ['GREATER LONDON', 'CLEVELAND', 'ESSEX']

    result = (
        df
        .filter(col("Country").isin(selected_countries))
        .select("transaction_id", "Price", "Country")
        .orderBy(col("Price").desc())
        .limit(2)
    )

    # # Display the result table
    # result.show()

    # Extract values for printing the result
    second_row_values = result.collect()[1]
    second_highest_price = second_row_values["Price"]
    country_name = second_row_values["Country"]

    # Record the end time
    end_time = time.time()

    # Calculate and print the execution time
    execution_time = end_time - start_time

    print(f"Second highest transacted value is {second_highest_price} which is transacted in {country_name}.")
    print(f"Program execution time: {execution_time} seconds")

except Exception as e:
    print(f"An unexpected error occurred: {str(e)}")

finally:
    # Stop the Spark session
    if 'mySpark' in locals():
        mySpark.stop()
