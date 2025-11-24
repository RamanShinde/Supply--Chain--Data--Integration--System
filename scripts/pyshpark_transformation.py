
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,IntegerType,StructField
from pyspark.sql.functions import col, to_date, datediff, round, avg, count,sum

# Configuration
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk-21"
os.environ["SPARK_HOME"] = "C:/Users/Shree/OneDrive/Desktop/SCM/SupplyChainDataIntegrationSystem/.venv/Lib/site-packages/pyspark"
os.environ["HADOOP_HOME"]="C:/Hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Create a spark session
spark=SparkSession.builder \
    .appName("Supply Chain Data Integration") \
    .getOrCreate()

#Load CSV file into Dataframes
vendor_df=spark.read.csv("../Data/vendor.csv",header=True,inferSchema=True)
sales_df=spark.read.csv("../Data/sales.csv",header=True,inferSchema=True)
purches_df=spark.read.csv("../Data/purchases.csv",header=True,inferSchema=True)

def run_transformations():
    # -------------------- VENDOR DATA CLEANING --------------------
    print("========== VENDOR DATA ==========")
    vendor_df.show(5)
    print(vendor_df.columns)
    print(vendor_df.dtypes)

    null_cnt = vendor_df.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in vendor_df.columns
    ])
    null_cnt.show()

    duplicates = vendor_df.groupby(vendor_df.columns).count().filter(col("count") > 1)
    duplicates.show()

    # Sample Data from dataset
    smaple_sales = sales_df.sample(withReplacement=False, fraction=0.01, seed=42)
    sample_purches = purches_df.sample(withReplacement=False, fraction=0.05, seed=42)

    # -------------------- SALES DATA CLEANING --------------------
    print("========== SALES DATA ==========")
    sales_df.show(5)
    print(sales_df.columns)
    print(sales_df.dtypes)
    sales_null = smaple_sales.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in sales_df.columns
    ])
    sales_null.show()

    sales_duplicate = smaple_sales.groupby(sales_df.columns).count().filter(col("count") > 1)
    sales_duplicate.show()

    # -------------------- PURCHASES DATA CLEANING --------------------
    print("========== PURCHASES DATA ==========")
    purches_df.show(5)
    print(purches_df.columns)
    print(purches_df.dtypes)
    smaple_purches = purches_df.select([
        sum(col(c).isNull().cast("int")).alias(c) for c in purches_df.columns
    ])
    smaple_purches.show()

    purches_duplicate = smaple_purches.groupby(purches_df.columns).count().filter(col("count") > 1)
    purches_duplicate.show()

    # -------------------- Calculate key metrics --------------------
    print("========== CALCULATE METRICS ==========")
    sample_purchases_matric = sample_purches.withColumn(
        "lead_time_days",
        datediff(to_date(col("PayDate"), "yyyy-MM-dd"), to_date(col("PODate"), "yyyy-MM-dd"))
    ).withColumn(
        "invoice_delay_days",
        datediff(to_date(col("PayDate"), "yyyy-MM-dd"), to_date(col("InvoiceDate"), "yyyy-MM-dd"))
    )

    sales_with_pruches = smaple_sales.join(
        smaple_purches.select("InventoryId", "PODate"),
        on="InventoryId",
        how="left"
    )

    sales_with_metrics = sales_with_pruches.withColumn(
        "Order_Cycle_days",
        datediff(to_date(col("SalesDate"), "yyyy-MM-dd"), to_date(col("PODate"), "yyyy-MM-dd"))
    )
    sales_with_metrics.printSchema()

    # Register views
    sample_purchases_matric.createOrReplaceTempView("purchases")
    sales_with_metrics.createOrReplaceTempView("sales")
    vendor_df.createOrReplaceTempView("vendor")

    # -------------------- AGGREGATED METRICS --------------------
    avg_lead_time_vendor = spark.sql("""
         select 
             VendorNumber,
             ROUND(AVG(lead_time_days), 2) as avg_lead_time_days,
             ROUND(AVG(invoice_delay_days),2) as avg_invoice_delay_days,
             count(PONumber) as total_orders
         from purchases
         group by VendorNumber
         order by VendorNumber
         limit 10    
    """)

    total_purchase_product = spark.sql("""
         select 
              InventoryId as productId,
              sum(Quantity) as total_purchased
         from purchases
         group by InventoryId     
    """)

    total_sales_product = spark.sql("""
        select 
            InventoryID as productId,
            Sum(SalesQuantity) as total_sold
        from sales
        group by InventoryId   
    """)

    total_purchase_product.createOrReplaceTempView("total_purchase_product")

    # Calculate remaining inventory
    inventory_df = spark.sql("""
         select
            p.productID,
            p.total_purchased,
            COALESCE(s.total_sold,0) as total_sold,
            p.total_purchased - COALESCE(s.total_sold, 0) AS inventory_remaining
        FROM total_purchase_product p
        LEFT JOIN (
            SELECT InventoryId AS productID, SUM(SalesQuantity) AS total_sold
            FROM sales
            GROUP BY InventoryId
        ) s
        ON p.productID = s.productID
    """)

    top_products = spark.sql("""
           select 
                InventoryId,
                SUM(SalesQuantity) as total_sales
           from sales
           group by InventoryId
           order by total_sales desc
           limit 10
    """)

    top_vendors_sql = spark.sql("""
            select 
                v.VendorNumber,
                v.VendorName,
                sum(s.SalesQuantity) as total_sales
            from sales s
            join vendor v
                on s.VendorNo = v.VendorNumber
            group by v.VendorNumber, v.VendorName
            order by total_sales desc
            limit 10         
    """)

    sales_clean = spark.sql("""
       select
            InventoryId,
            SalesQuantity,
            to_date(SalesDate, 'yyyy-MM-dd') AS SalesDate,
            YEAR(to_date(SalesDate, 'yyyy-MM-dd')) AS Year,
            MONTH(to_date(SalesDate, 'yyyy-MM-dd')) AS Month
       from sales     
    """)

    sales_clean.createOrReplaceTempView("sales_year")

    year_wise_sales = spark.sql("""
         select 
            Year,
            SUM(SalesQuantity) as totak_sales_qty,
            COUNT(*) as total_orders
         From sales_year
         group by year
         order by year       
    """)

    top_monthly_sql = spark.sql("""
          select 
              month,
              InventoryId,
              SUM(SalesQuantity) as total_sales
          from sales_year
          group by month, InventoryId
          order by month, total_sales desc    
    """)

    top_5_product = spark.sql("""
        select 
            Year,
            InventoryId,
            SUM(SalesQuantity) as total_sales
        from sales_year
        group by Year, InventoryId
        order by Year, total_sales desc
        limit 5    
    """)

    least_5_product = spark.sql("""
        select 
            Year,
            InventoryId,
            SUM(SalesQuantity) as total_sales
        from sales_year
        group by Year, InventoryId
        order by Year, total_sales asc
        limit 5    
    """)

    top_purchases_product = spark.sql(""" 
          select
             InventoryId,
             SUM(Quantity) as total_purchased
          from purchases
          group by InventoryId
          order by total_purchased desc
          limit 10   
    """)
    # Convert to Pandas
    return {
        "vendor_metrics": avg_lead_time_vendor.toPandas(),
        "total_purchase": total_purchase_product.toPandas(),
        "total_sales": total_sales_product.toPandas(),
        "inventory": inventory_df.toPandas(),
        "top_products": top_products.toPandas(),
        "top_vendors": top_vendors_sql.toPandas(),
        "year_wise_sales": year_wise_sales.toPandas(),
        "top_monthly": top_monthly_sql.toPandas(),
        "top_5_products": top_5_product.toPandas(),
        "least_5": least_5_product.toPandas(),
        "top_product_purchases": top_purchases_product.toPandas()
    }

def convert_into_pandas():
    return run_transformations()

if __name__ == "__main__":
    run_transformations()
