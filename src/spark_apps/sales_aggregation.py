"""
Sales Aggregation Spark Application.

Aggregates sales data from the mock warehouse database.
Demonstrates Spark integration with PostgreSQL and data processing.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg, max, min, col


def main():
    """Run sales aggregation Spark job."""
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: sales_aggregation.py <warehouse_jdbc_url> [output_path]")
        print("Example: sales_aggregation.py jdbc:postgresql://warehouse:5432/warehouse /tmp/sales_output")
        sys.exit(1)

    warehouse_url = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "/tmp/sales_aggregation_output"

    # Database connection properties
    db_properties = {
        "user": "warehouse_user",
        "password": "warehouse_pass",
        "driver": "org.postgresql.Driver",
    }

    # Create Spark session
    spark = (
        SparkSession.builder.appName("SalesAggregation")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.5.0")
        .getOrCreate()
    )

    print(f"Spark version: {spark.version}")
    print(f"Warehouse URL: {warehouse_url}")
    print(f"Output path: {output_path}")

    try:
        # Read fact sales table
        print("\nReading FactSales table...")
        fact_sales = spark.read.jdbc(
            url=warehouse_url, table="FactSales", properties=db_properties
        )

        print(f"Loaded {fact_sales.count()} sales records")
        fact_sales.printSchema()

        # Read dimension tables
        print("\nReading dimension tables...")
        dim_customer = spark.read.jdbc(
            url=warehouse_url, table="DimCustomer", properties=db_properties
        )
        dim_product = spark.read.jdbc(
            url=warehouse_url, table="DimProduct", properties=db_properties
        )
        dim_date = spark.read.jdbc(
            url=warehouse_url, table="DimDate", properties=db_properties
        )

        # Join fact with dimensions
        print("\nJoining fact table with dimensions...")
        sales_enriched = (
            fact_sales.join(dim_customer, "CustomerKey")
            .join(dim_product, "ProductKey")
            .join(dim_date, fact_sales.OrderDateKey == dim_date.DateKey)
        )

        # Aggregate by product category
        print("\n=== Sales by Product Category ===")
        category_sales = (
            sales_enriched.groupBy("Category")
            .agg(
                count("SalesKey").alias("num_orders"),
                sum("Quantity").alias("total_quantity"),
                sum("TotalAmount").alias("total_revenue"),
                avg("TotalAmount").alias("avg_order_value"),
            )
            .orderBy(col("total_revenue").desc())
        )

        category_sales.show(truncate=False)

        # Aggregate by customer segment
        print("\n=== Sales by Customer Segment ===")
        segment_sales = (
            sales_enriched.groupBy("CustomerSegment")
            .agg(
                count("SalesKey").alias("num_orders"),
                sum("TotalAmount").alias("total_revenue"),
                avg("TotalAmount").alias("avg_order_value"),
            )
            .orderBy(col("total_revenue").desc())
        )

        segment_sales.show(truncate=False)

        # Monthly sales trend
        print("\n=== Monthly Sales Trend (Last 12 months) ===")
        monthly_sales = (
            sales_enriched.groupBy("Year", "Month")
            .agg(
                count("SalesKey").alias("num_orders"),
                sum("TotalAmount").alias("total_revenue"),
            )
            .orderBy("Year", "Month", ascending=[False, False])
            .limit(12)
        )

        monthly_sales.show(truncate=False)

        # Top products by revenue
        print("\n=== Top 10 Products by Revenue ===")
        top_products = (
            sales_enriched.groupBy("ProductKey", "ProductName", "Category")
            .agg(
                sum("TotalAmount").alias("total_revenue"),
                sum("Quantity").alias("total_quantity"),
            )
            .orderBy(col("total_revenue").desc())
            .limit(10)
        )

        top_products.show(truncate=False)

        # Save aggregated results
        print(f"\nSaving results to {output_path}...")

        category_sales.write.mode("overwrite").parquet(f"{output_path}/category_sales")
        segment_sales.write.mode("overwrite").parquet(f"{output_path}/segment_sales")
        monthly_sales.write.mode("overwrite").parquet(f"{output_path}/monthly_sales")
        top_products.write.mode("overwrite").parquet(f"{output_path}/top_products")

        print("Results saved successfully")

        # Summary statistics
        print("\n=== Summary Statistics ===")
        print(f"Total orders: {fact_sales.count()}")
        print(f"Total revenue: ${fact_sales.agg(sum('TotalAmount')).collect()[0][0]:,.2f}")
        print(f"Average order value: ${fact_sales.agg(avg('TotalAmount')).collect()[0][0]:,.2f}")
        print(f"Number of customers: {dim_customer.count()}")
        print(f"Number of products: {dim_product.count()}")

    except Exception as e:
        print(f"Error during sales aggregation: {str(e)}")
        import traceback

        traceback.print_exc()
        raise
    finally:
        spark.stop()
        print("\nSpark session stopped")


if __name__ == "__main__":
    main()