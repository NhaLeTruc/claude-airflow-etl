"""
Simple Word Count Spark Application.

Demonstrates basic PySpark functionality for testing Spark operators.
Counts word frequencies from input text.
"""

import sys
from pyspark.sql import SparkSession


def main():
    """Run word count Spark job."""
    # Parse arguments
    if len(sys.argv) < 3:
        print("Usage: word_count.py <input_path> <output_path>")
        print("Using default values for demo")
        input_path = None
        output_path = "/tmp/wordcount_output"
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]

    # Create Spark session
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    print(f"Spark version: {spark.version}")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")

    try:
        if input_path:
            # Read from file
            text_df = spark.read.text(input_path)
        else:
            # Create sample data for demo
            sample_data = [
                ("Apache Spark is a unified analytics engine",),
                ("Spark provides high-level APIs in Java Scala Python and R",),
                ("Spark is fast and general purpose cluster computing system",),
                ("Apache Airflow orchestrates Spark jobs",),
            ]
            text_df = spark.createDataFrame(sample_data, ["value"])

        # Split lines into words
        from pyspark.sql.functions import explode, split, lower, trim

        words_df = text_df.select(explode(split(lower(trim(text_df.value)), "\\s+")).alias("word"))

        # Filter empty strings
        words_df = words_df.filter(words_df.word != "")

        # Count word frequencies
        word_counts = words_df.groupBy("word").count().orderBy("count", ascending=False)

        # Show results
        print("\n=== Word Count Results ===")
        word_counts.show(20, truncate=False)

        # Write results
        word_counts.write.mode("overwrite").csv(output_path)
        print(f"\nResults written to: {output_path}")

        # Print summary
        total_words = words_df.count()
        unique_words = word_counts.count()
        print(f"\nTotal words: {total_words}")
        print(f"Unique words: {unique_words}")

    except Exception as e:
        print(f"Error during word count: {str(e)}")
        raise
    finally:
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()