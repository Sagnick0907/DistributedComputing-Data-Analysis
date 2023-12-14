from pyspark.sql import SparkSession
import time

try:
    # Start the timer
    start_time = time.time()

    # Initialize a Spark session
    mySpark = SparkSession.builder.appName("CountryWithSecondMostTransactions").getOrCreate()

    # Load the CSV file into an RDD
    csv_file_path = "House_Pricing.csv"
    rdd = mySpark.sparkContext.textFile(csv_file_path)

    # Split each line of the CSV file
    header = rdd.first()
    rdd = rdd.filter(lambda line: line != header).map(lambda line: line.split(','))

    # Extract only the specified columns: 'Transaction unique identifier', 'Price', 'Country'
    rdd = rdd.map(lambda x: (x[0], x[1], x[8]))

    # Create a DataFrame with the specified columns
    columns = ["TransactionID", "Price", "Country"]
    df = mySpark.createDataFrame(rdd, columns)

    # Register the DataFrame as a temporary table
    df.createOrReplaceTempView("house_pricing")

    # Use Spark SQL to find the country with the second most transactions
    second_most_transactions_query = """
        SELECT Country, COUNT(*) AS TransactionCount
        FROM house_pricing
        GROUP BY Country
        ORDER BY TransactionCount DESC
        LIMIT 2
    """

    finalResult = mySpark.sql(second_most_transactions_query)

    # Extract the country name from the result
    second_most_transactions_country = finalResult.collect()[1]["Country"]

    # Stop the timer
    end_time = time.time()

    # Calculate and display the execution time
    execution_time = end_time - start_time

    # Display the result
    print(f"Country with second most transactions is '{second_most_transactions_country}'.")
    
    print(f"Execution time: {execution_time} seconds.")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Stop the Spark session
    mySpark.stop()
