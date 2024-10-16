# word_count.py
from pyspark.sql import SparkSession

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("WordCountApp") \
        .getOrCreate()

    # Path to the input file (replace with your own file path if necessary)
    input_path = "/opt/spark/data/sample.txt"

    # Read the input file
    text_file = spark.read.text(input_path).rdd

    # Perform word count
    word_counts = text_file.flatMap(lambda line: line[0].split(" ")) \
                          .map(lambda word: (word, 1)) \
                          .reduceByKey(lambda a, b: a + b)

    # Collect and print the result
    for word, count in word_counts.collect():
        print(f"{word}: {count}")

    # Stop the Spark session
    spark.stop()