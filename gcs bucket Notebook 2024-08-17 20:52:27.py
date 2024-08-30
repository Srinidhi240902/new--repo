# Databricks notebook source

# Set up the configurations for mounting the GCS bucket
gcs_bucket_name = "enquelt"
mount_point = "/mnt/srinidhiecomdata"
project_id="mentorsko-1723044061533"
service_account_key = "/dbfs/FileStore/tables/mentorsko_1723044061533_3e38ffcd3e68.json"

# Define the GCS service account credentials
config = {
"fs.gs.project.id": project_id,
"fs.gs.auth.service.account.json.keyfile": service_account_key # keyfile should be indicating to the abs path of credentials.json
}


# Mount the GCS bucket
dbutils.fs.mount(
    source = f"gs://{gcs_bucket_name}",
    mount_point = mount_point,
    extra_configs = config
)

# Display the contents of the mounted directory to verify
display(dbutils.fs.ls(mount_point))




# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.unmount("/mnt/srinidhiecomdata")


# COMMAND ----------



# COMMAND ----------

orders_path='dbfs:/mnt/srinidhiecomdata/orders.csv'
orders_df= spark.read.option("header","true").option("inferSchema","true").csv(orders_path)

# COMMAND ----------

suppliers_df =spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/srinidhiecomdata/suppliers.csv")

# COMMAND ----------

shipping_teir_df= spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/srinidhiecomdata/shipping_tier.csv")

# COMMAND ----------

customers_df= spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/srinidhiecomdata/customers.csv")

# COMMAND ----------

products_df= spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/srinidhiecomdata/products.csv")

# COMMAND ----------

order_items_df= spark.read.format("csv").option("header","true").option("inferSchema","true").load("dbfs:/mnt/srinidhiecomdata/orders_items.csv")

# COMMAND ----------

orders_df.createOrReplaceGlobalTempView("orders")

# COMMAND ----------

order_items_df.createOrReplaceGlobalTempView("order_items")

# COMMAND ----------

shipping_teir_df.createOrReplaceGlobalTempView("shipping_teir")

# COMMAND ----------

customers_df.createOrReplaceGlobalTempView("customers")

# COMMAND ----------

products_df.createOrReplaceGlobalTempView("products")

# COMMAND ----------

# MAGIC %sql
# MAGIC select ShippingTierID, count(OrderID) from orders
# MAGIC group by ShippingTierID

# COMMAND ----------

filtered_df = products_df.filter(products_df.Product_Rating >= 4.5)


filtered_df.display()

# COMMAND ----------

from pyspark.sql.functions import datediff
result_df = orders_df.withColumn("days_difference", 
                                 datediff(orders_df.ShippingDate, orders_df.OrderDate))


# COMMAND ----------

# MAGIC %sql with ac as(select  p.product_id,o.OrderDate, case when p.Discounted_Price is null then cast(replace(replace(p.Actual_Price, '₹', ''), ',', '') as int)*i.Quantity
# MAGIC else cast(replace(replace(p.Discounted_Price, '₹', ''), ',', '') as int)*i.Quantity
# MAGIC end as sales from order_items i join products p 
# MAGIC on p.product_id=i.productID
# MAGIC join orders o
# MAGIC on  o.OrderID = i.OrderID
# MAGIC
# MAGIC
# MAGIC )
# MAGIC select product_id,OrderDate, ifnull(sum(sales) over(partition by product_id ),0) as running_sales from ac
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, sum as spark_sum, year, month, lag, round
from pyspark.sql.window import Window




products1_df = (
    products_df.join(order_items_df, products_df.Product_ID == order_items_df.ProductID)
    .select(
        "*",
        when(
            col("Discounted_Price").isNull(),
            regexp_replace(regexp_replace(col("Actual_Price"), '₹', ''), ',', '').cast("int") * col("Quantity")
        ).otherwise(
            regexp_replace(regexp_replace(col("Discounted_Price"), '₹', ''), ',', '').cast("int") * col("Quantity")
        ).alias("sale")
    )
)


ss_df = (
    orders_df.join(order_items_df, orders_df.OrderID == order_items_df.OrderID)
    .join(products1_df, products1_df.ProductID == order_items_df.ProductID)
    .groupBy(year(col("OrderDate")).alias("year"), month(col("OrderDate")).alias("month"))
    .agg(spark_sum("sale").alias("tot_value"))
)


window_spec = Window.partitionBy("year").orderBy("month")

ss1_df = (
    ss_df.withColumn("prev", lag("tot_value").over(window_spec))
)


result_df = (
    ss1_df.select(
        col("year"),
        col("month"),
        round((col("tot_value") - col("prev")) / col("prev"), 2).alias("momper")
    )
)


result_df.display()


# COMMAND ----------

suppliers_df.write.saveAsTable("suppliers1")

# COMMAND ----------

# MAGIC %sql with s as(select o.OrderID,s.SupplierID,date(o.OrderDate) as orderdate from orders o join suppliers1 s
# MAGIC on o.supplierid = s.SupplierID)
# MAGIC select SupplierID,Orderdate, count(orderid) over(partition by supplierid  order by Orderdate  ) as cnt from s

# COMMAND ----------

# MAGIC %sql
# MAGIC  with products1 as( select*,case when p.Discounted_Price is null then cast(replace(replace(p.Actual_Price, '₹', ''), ',', '') as int)*i.Quantity
# MAGIC else cast(replace(replace(p.Discounted_Price, '₹', ''), ',', '') as int)*i.Quantity end as sale from products p join order_items i
# MAGIC on p.Product_ID= i.ProductID
# MAGIC )
# MAGIC , ss as(
# MAGIC select year(o.Orderdate)as year, month(o.OrderDate) as month, sum(p.sale) as tot_value from  orders o
# MAGIC join order_items i
# MAGIC on i.OrderID=o.OrderID
# MAGIC join products1 p
# MAGIC on p.ProductID=i.ProductID
# MAGIC group by year(o.OrderDate), month(o.OrderDate)
# MAGIC ),ss1 as(
# MAGIC select*, lag(tot_value)over(partition by year order by month) as prev from ss
# MAGIC )
# MAGIC select year, month,round((tot_value-prev)/prev,2) as momper from ss1

# COMMAND ----------

# Step 1: Adjusted to avoid ambiguity by selecting necessary columns explicitly
products1_df = (
    products_df.join(order_items_df, products_df.Product_ID == order_items_df.ProductID)
    .select(
        products_df["*"],  # Select all columns from products_df
        order_items_df["Quantity"],  # Explicitly select Quantity from order_items_df
        when(
            col("Discounted_Price").isNull(),
            regexp_replace(regexp_replace(col("Actual_Price"), '₹', ''), ',', '').cast("int") * col("Quantity")
        ).otherwise(
            regexp_replace(regexp_replace(col("Discounted_Price"), '₹', ''), ',', '').cast("int") * col("Quantity")
        ).alias("sale")
    )
)

# Step 2: Adjusted to avoid ambiguity by selecting necessary columns explicitly after join
pa_df = (
    orders_df.join(order_items_df, orders_df.OrderID == order_items_df.OrderID)
    .join(products1_df, products1_df.Product_ID == order_items_df.ProductID)
    .select(
        products1_df["Product_ID"].alias("product_id"), 
        products1_df["Product_Name"].alias("product_name"),
        order_items_df["OrderID"],  # Explicitly select OrderID from order_items_df
        order_items_df["Quantity"],  # Explicitly select Quantity from order_items_df
        products1_df["sale"]  # Explicitly select sale from products1_df
    )
    .groupBy("product_id", "product_name")
    .agg(
        countDistinct("OrderID").alias("tot_orders"),
        spark_sum("Quantity").alias("tot_units"),
        spark_sum("sale").alias("tot_revenue")
    )
)

# Step 3: Adjusted to avoid ambiguity by selecting necessary columns explicitly after join
pb_df = (
    returns_df.join(order_items_df, returns_df.OrderID == order_items_df.OrderID)
    .join(products_df, products_df.Product_ID == order_items_df.ProductID)
    .select(
        products_df["Product_ID"].alias("product_id"), 
        products_df["Product_Name"].alias("product_name"),
        order_items_df["OrderID"]  # Explicitly select OrderID from order_items_df
    )
    .groupBy("product_id", "product_name")
    .agg(
        countDistinct("OrderID").alias("returns")
    )
)

# Step 4: Join `pa` and `pb` DataFrames and calculate `return_rate` and `avg_price`
result_df = (
    pa_df.join(pb_df, pa_df.product_id == pb_df.product_id, how='left')
    .select(
        pa_df.product_id,
        pa_df.product_name,  # Use pa_df to select product_name to avoid ambiguity
        pa_df.tot_orders,
        pa_df.tot_revenue,
        pa_df.tot_units,
        pb_df.returns,
        (col("returns") / col("tot_orders")).alias("return_rate"),
        (col("tot_revenue") / col("tot_units")).alias("avg_price")
    )
)

# Show the results
display(result_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH rows AS (
# MAGIC   SELECT
# MAGIC     c.CustomerID,c.FirstName, c.LastName,
# MAGIC     o.OrderID,
# MAGIC     o.OrderDate,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.CustomerID ORDER BY o.OrderDate desc) AS rn
# MAGIC   FROM customers c
# MAGIC   JOIN orders o ON c.CustomerID = o.CustomerID
# MAGIC )
# MAGIC SELECT
# MAGIC   CustomerID,FirstName, Lastname
# MAGIC   OrderID,
# MAGIC   OrderDate
# MAGIC FROM rows
# MAGIC WHERE rn = 7

# COMMAND ----------

# MAGIC %sql with aa as(
# MAGIC  SELECT
# MAGIC     c.CustomerID,
# MAGIC     YEAR(o.OrderDate) as purchase_year,
# MAGIC     MONTH(o.OrderDate) as purchase_month,
# MAGIC     o.OrderID
# MAGIC     
# MAGIC     
# MAGIC   FROM customers c
# MAGIC   JOIN orders o ON c.CustomerID = o.CustomerID
# MAGIC )
# MAGIC select customerid, purchase_year, purchase_month, count(orderid)as order_no from aa
# MAGIC group by CustomerID, purchase_year,purchase_month

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC with aa as(
# MAGIC  SELECT
# MAGIC     c.CustomerID,
# MAGIC     YEAR(o.OrderDate) as purchase_yr,
# MAGIC     MONTH(o.OrderDate) as purchase_mnth,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY c.CustomerID ORDER BY o.OrderDate) as rn
# MAGIC   FROM hive_metastore.default.customers c
# MAGIC   JOIN hive_metastore.default.orders o ON c.CustomerID = o.CustomerID
# MAGIC ), ab as(
# MAGIC   select purchase_yr, purchase_mnth, count(distinct CustomerID) as total_customers 
# MAGIC   from aa
# MAGIC   group by purchase_yr, purchase_mnth
# MAGIC ), ac as (
# MAGIC   select purchase_yr, purchase_mnth, count(distinct CustomerID) as first_customer 
# MAGIC   from aa
# MAGIC   where rn = 1
# MAGIC   group by purchase_yr, purchase_mnth
# MAGIC ), ad as (
# MAGIC   select purchase_yr, purchase_mnth, count(distinct CustomerID) as repeat_customer 
# MAGIC   from aa
# MAGIC   where rn > 1
# MAGIC   group by purchase_yr, purchase_mnth
# MAGIC )
# MAGIC select ab.purchase_yr, ab.purchase_mnth, ab.total_customers, ac.first_customer, ad.repeat_customer, round(ad.repeat_customer*100.0/ab.total_customers, 2) as repeat_rate
# MAGIC from ab
# MAGIC left join ac on ab.purchase_yr = ac.purchase_yr and ab.purchase_mnth = ac.purchase_mnth
# MAGIC left join ad on ab.purchase_yr = ad.purchase_yr and ab.purchase_mnth = ad.purchase_mnth
# MAGIC where (ab.purchase_yr= 1996 AND ab.purchase_mnth = 8) OR
# MAGIC       (ab.purchase_yr= 2008 AND ab.purchase_mnth = 11) OR
# MAGIC       (ab.purchase_yr= 1994 AND ab.purchase_mnth = 1) OR
# MAGIC       (ab.purchase_yr= 2024 AND ab.purchase_mnth = 6)
# MAGIC order by repeat_rate desc
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ReturnData AS (
# MAGIC   SELECT
# MAGIC     o.OrderID,
# MAGIC     CASE WHEN r.OrderId IS NOT NULL THEN 1 ELSE 0 END AS IsReturned
# MAGIC   FROM hive_metastore.default.orders o
# MAGIC   LEFT JOIN hive_metastore.default.returns r ON o.OrderID = r.OrderId
# MAGIC   WHERE YEAR(o.OrderDate) = 2023 AND MONTH(o.OrderDate) = 7
# MAGIC )
# MAGIC SELECT
# MAGIC   SUM(IsReturned) * 100.0 
# MAGIC   -- COUNT(*), 2) AS ReturnRate
# MAGIC FROM ReturnData

# COMMAND ----------

suppliers_df.display()

# COMMAND ----------

orders_df.display()

# COMMAND ----------

from pyspark.sql import functions as F


orders_df = orders_df.withColumn("on_time", F.when(F.col("ActualDeliveryDate") <= F.col("ExpectedDeliveryDate"), 1).otherwise(0))
orders_df = orders_df.withColumn("delayed", F.when(F.col("ActualDeliveryDate") > F.col("ExpectedDeliveryDate"), 1).otherwise(0))
orders_df = orders_df.withColumn("delay_in_days", F.when(F.col("delayed") == 1, F.datediff(F.col("ActualDeliveryDate"), F.col("ExpectedDeliveryDate"))).otherwise(0))

df = orders_df.groupBy("SupplierID", "ShippingTierID").agg(
    F.sum("on_time").alias("on_time_deliveries"),
    F.sum("delayed").alias("late_deliveries"),
    F.avg("delay_in_days").alias("average_delay"),
)

df = df.withColumn("delaydelivery_rate", F.col("late_deliveries") / (F.col("late_deliveries") + F.col("on_time_deliveries")))
df = df.withColumn("ontimedelivery_rate", F.col("on_time_deliveries") / (F.col("late_deliveries") + F.col("on_time_deliveries")))

df.display()

# COMMAND ----------

from pyspark.sql import functions as F

df_joined = df.join(suppliers_df, "SupplierID", "inner") \
              .join(shipping_teir_df, "ShippingTierID", "inner") \
              .select(df["*"], suppliers_df["SupplierName"], shipping_teir_df["TierName"])

df_joined.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Filter orders from "Johnson & Co." and join with shipping tier information
johnson_orders = orders_df.join(suppliers_df, "SupplierID") \
                          .join(shipping_teir_df, "ShippingTierID") \
                          .filter(F.col("SupplierName") == "Johnson & Co.")

# Group by ShippingTierID and count the number of orders
johnson_orders_grouped = johnson_orders.groupBy("ShippingTierID", "TierName") \
                                       .agg(F.count("OrderID").alias("number_of_orders"))

# Find the delivery tier with the highest number of orders
highest_orders_tier = johnson_orders_grouped.orderBy(F.desc("number_of_orders")).limit(1)

display(highest_orders_tier)

# COMMAND ----------

from pyspark.sql.functions import input_file_name


bronze_table_path = "/mnt/delta/bronze/my_table"

df = spark.readStream.format("cloudFiles") \
  .option("cloudFiles.format", "csv") \
  .option("cloudFiles.inferColumnTypes", "true") \
  .option("header", "true") \
  .load("dbfs:/FileStore/my_data_source")

df.writeStream.format("delta") \
  .option("checkpointLocation", "/mnt/delta/bronze/checkpoints/my_table") \
  .start(bronze_table_path)

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_table_path)

completeness_check = bronze_df.select([count(when(col(c).isNull(), c)).alias(c) for c in bronze_df.columns])

uniqueness_check = bronze_df.groupBy("YourKeyColumn").count().where("count > 1")



# COMMAND ----------


