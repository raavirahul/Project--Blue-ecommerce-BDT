# Databricks notebook source
# MAGIC %md
# MAGIC ## BLU - Group 6 

# COMMAND ----------

team_members = ["ONUOHA Bessy", "XIAO Ying", "RAAVI Rahul"]
academic_year = "2024"
course_name = "Data Science Project"

# COMMAND ----------

product_path = "/FileStore/tables/products.csv"
test_product_path = "/FileStore/tables/test_products.csv"
orders_path = "/FileStore/tables/orders.csv"
test_orders_path = "/FileStore/tables/test_orders.csv"
items_path = "/FileStore/tables/order_items.csv"
test_items_path = "/FileStore/tables/test_order_items.csv"
payments_path = "/FileStore/tables/order_payments.csv"
test_payments_path = "/FileStore/tables/test_order_payments.csv"
order_review_path = "/FileStore/tables/order_reviews.csv"

# COMMAND ----------

#There are five data sources:
"""
  products
  orders
  order_items
  order_payments
  order_reviews
"""
# There are four hold out data sources: 
"""
  test_products 
  test_orders
  test_order_items
  test_order_payments
"""

#Load functions
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### ----- Part 1: Data Preparation, Data Understanding and Basetable Creation -----

# COMMAND ----------

# DBTITLE 1,Table: products & test_products
"""
Metadata description: 
  •	product_id: Product unique identifier
  •	product_name_length: Number of characters in the product name
  •	product_description_length: Number of characters in the product description
  •	product_photos_qty: Number of photos included in the product description
  •	product_weight_g: Product weight (in grams)
  •	product_length_cm: Product dimensions - length (in centimeters)
  •	product_height_cm: Product dimensions - height (in centimeters)
  •	product_width_cm: Product dimensions - width (in centimeters)
  •	product_category_name: Product category name
"""

# COMMAND ----------


#read in the product and test_product dataset
product = spark.read.format("csv").option("header", "True").option("inferSchema", "True").option("escape", "\"").load(product_path)
test_product = spark.read.format("csv").option("header", "True").option("inferSchema", "True").option("escape", "\"").load(test_product_path)

product.show(2)
product.printSchema()
product.describe().show()
product.dropDuplicates()

test_product.show(2)
test_product.printSchema()
test_product.describe().show()
test_product.dropDuplicates()

# COMMAND ----------

#check for null and na values in the product and test_product dataset
product.select([count(when(isnull(c)| isnan(c), c)).alias(c) for c in product.columns]).show()
test_product.select([count(when(isnull(c)| isnan(c), c)).alias(c) for c in test_product.columns]).show()

# COMMAND ----------

#Creating new variables for the product and test_product tables

# length, m
product = product.withColumn("product_length", col("product_length_cm")/100).drop("product_length_cm")
test_product = test_product.withColumn("product_length", col("product_length_cm") /100).drop("product_length_cm")

# height, m
product = product.withColumn("product_height", col("product_height_cm")/100).drop("product_height_cm")
test_product = test_product.withColumn("product_height", col("product_height_cm")/100).drop("product_height_cm")

# width, m
product = product.withColumn("product_width", col("product_width_cm")/100).drop("product_width_cm")
test_product = test_product.withColumn("product_width", col("product_width_cm")/100).drop("product_width_cm")

# Volume, m3
product = product.withColumn("Prod_volume_m3", round(col("product_length") * col("product_height") * col("product_width"),2))
test_product = test_product.withColumn("Prod_volume_m3", round(col("product_length") * col("product_height") * col("product_width"),2))

# Weight in kg
product = product.withColumn("weight_kg", round(col("product_weight_g") /1000, 2)).drop("product_weight_g")
test_product = test_product.withColumn("weight_kg", round(col("product_weight_g") / 1000, 2)).drop("product_weight_g")


# COMMAND ----------

product.show(2)
test_product.show(2)

# COMMAND ----------

# DBTITLE 1,Table: orders & test_orders
"""
Metadata description: 
  •	order_id: Order unique identifier
  •	customer_id: Unique identifier of the customer
  •	order_status: Status of the order
  •	order_purchase_timestamp: Timestamp at which the customer purchased the order
  •	order_approved_at: Timestamp at which the order was verified and approved by the company
  •	order_delivered_carrier_date: Timestamp at which the order was delivered by the company to the logistic partner
  •	order_delivered_customer_date: Timestamp at which the order was delivered to the customer by the logistic partner 
  •	order_estimated_delivery_date: Estimated delivery date that was communicated to the customer at the purchase moment
"""

# COMMAND ----------

#Read table: orders
orders = spark.read.format("csv").option("header", "true").option("multiline", "true").load(orders_path)
orders.show(5)
orders.printSchema()
orders.dropDuplicates()

#Read table: test_orders
test_orders = spark.read.format("csv").option("header", "true").option("multiline", "true").load(test_orders_path)
test_orders.show(5)
test_orders.printSchema()
test_orders.dropDuplicates()


# COMMAND ----------

#Count the number of nulls per column for orders 
orders.select([count(when(col(c).isNull(), c)).alias(c) for c in orders.columns]).show()
#Count the number of nulls per column for test_orders 
test_orders.select([count(when(col(c).isNull(), c)).alias(c) for c in test_orders.columns]).show()

# remove where orderid is NA
orders =orders.where(orders["order_id"] != "NA")
test_orders = test_orders.where(test_orders['order_id']!= "NA")

# COMMAND ----------

# Calculate the weekly purchases on the day as per the timestamp (Mon=1, Sun=7)
orders = orders.withColumn("orders_of_week", dayofweek("order_purchase_timestamp"))
test_orders = test_orders.withColumn("orders_of_week", dayofweek("order_purchase_timestamp"))

# Create a new variable for weekend delivery where 1=weekend delivery and 0=weekday delivery
orders = orders.withColumn("weekend_delivered", when((dayofweek("order_delivered_customer_date") >= 6), 1).otherwise(0)).drop("orders_of_week")
test_orders = test_orders.withColumn("weekend_delivered", when((dayofweek("order_delivered_customer_date") >= 6), 1).otherwise(0)).drop("orders_of_week")

# Time gap between order purchase timestamp and order approved at (efficiency of the supplier) in hours
orders = orders.withColumn("approve_efficiency", round((unix_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) / 3600,2))
test_orders = test_orders.withColumn("approve_efficiency", round((unix_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) / 3600,2))

# Time gap between order approved at and order delivered carrier date (supplier packaging efficiency) in hours
orders = orders.withColumn("package_efficiency", round((unix_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")) / 3600,2))
test_orders = test_orders.withColumn("package_efficiency",round((unix_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_approved_at", "yyyy-MM-dd HH:mm:ss")) / 3600,2))

# Time gap between delivered carrier date and delivered customer date (efficiency of delivery) in hours
orders = orders.withColumn("delivery_efficiency",round((unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss")) / 3600,2))
test_orders = test_orders.withColumn("delivery_efficiency", round((unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_delivered_carrier_date", "yyyy-MM-dd HH:mm:ss")) / 3600,2))

# Time gap between order delivered customer date and estimated delivery date (check if there is overtime shipping) in hours
orders = orders.withColumn("on_time", round((unix_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")) / 3600,2))
test_orders = test_orders.withColumn("on_time", round((unix_timestamp("order_estimated_delivery_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss")) / 3600,2))

# Time gap between order delivered customer date and order purchase timestamp in hours (total time used for the customer to receive their delivery)
orders = orders.withColumn("total_delivery_time", round((unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) / 3600,2))
test_orders = test_orders.withColumn("total_delivery_time", round((unix_timestamp("order_delivered_customer_date", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("order_purchase_timestamp", "yyyy-MM-dd HH:mm:ss")) / 3600,2))

# drop unnecessary columns 
orders = orders.drop("order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date","order_hour")
test_orders = test_orders.drop("order_purchase_timestamp","order_approved_at","order_delivered_carrier_date","order_delivered_customer_date","order_estimated_delivery_date","order_hour")

# Convert all numeric columns to double()
orders = orders.select(col("order_id"), col("customer_id"), col("weekend_delivered").cast("double"), col("approve_efficiency").cast("double"), col("package_efficiency").cast("double"), col("delivery_efficiency").cast("double"), col("on_time").cast("double"),col("total_delivery_time").cast("double"))
test_orders = test_orders.select(col("order_id"), col("customer_id"), col("weekend_delivered").cast("double"), col("approve_efficiency").cast("double"), col("package_efficiency").cast("double"), col("delivery_efficiency").cast("double"),col("on_time").cast("double"), col("total_delivery_time").cast("double"))

# COMMAND ----------

orders.show(5)
test_orders.show(5)

# COMMAND ----------

orders.printSchema()

# COMMAND ----------

# DBTITLE 1,Table: order_items & test_order_items
"""
Metadata description: 
  •	order_id: Order unique idnetifier 
  •	order_item_id: Sequential number identifying the order of the ordered items. A customer can order multiple items per order.
  •	product_id: Product unique identifier 
  •	price: Item price in euro (excl. VAT)
  •	shipping_cost: Cost for shipping the item to the customer in euro (excl. VAT)
"""

# COMMAND ----------

#Read table: order_items 
items =spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(items_path)

items.show(3)
items.printSchema()
items.count()
items.dropDuplicates()

#Read table: test_order_items 
test_items =spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(test_items_path)

test_items.show(3)
test_items.printSchema()
test_items.count()
test_items.dropDuplicates()

# COMMAND ----------

#Count the number of nulls per column for order_items
items.select([count(when(col(c).isNull(), c)).alias(c) for c in items.columns]).show()
#Count the number of nulls per column for test_order_items
test_items.select([count(when(col(c).isNull(), c)).alias(c) for c in test_items.columns]).show()

# COMMAND ----------

# Calculate total price, shipping cost, and total cost grouped by orderid
items_nv = items.groupBy("order_id").agg(
    round(sum("price"),2).alias("total_price"),
    round(sum("shipping_cost"),2).alias("total_shipping_cost"),
    round((sum("price") + sum("shipping_cost")),2).alias("total_cost"),
    round(sum("shipping_cost")/(sum("price") + sum("shipping_cost")),2).alias("shipping_cost%"),
    max("order_item_id").alias("max_order_item_id"),
    countDistinct("product_id").alias("num_unique_products_per_id"))

test_items_nv = test_items.groupBy("order_id").agg(
    round(sum("price"),2).alias("total_price"),
    round(sum("shipping_cost"),2).alias("total_shipping_cost"),
    round((sum("price") + sum("shipping_cost")),2).alias("total_cost"),
    round(sum("shipping_cost")/(sum("price") + sum("shipping_cost")),2).alias("shipping_cost%"),
    max("order_item_id").alias("max_order_item_id"),
    countDistinct("product_id").alias("num_unique_products_per_id"))

# Join the calculated values back to the original DataFrame
items = items.join(items_nv, "order_id")
test_items = test_items.join(test_items_nv, "order_id")

# Drop unneccessary columns 
items = items.drop("order_item_id","price","shipping_cost")
test_items = test_items.drop("order_item_id","price","shipping_cost")

items = items.select(col("order_id"), col("product_id"), col("total_price").cast("double"), col("total_shipping_cost").cast("double"), col("total_cost").cast("double"), col("shipping_cost%").cast("double"), col("max_order_item_id").cast("double"), col("num_unique_products_per_id").cast("double"))
test_items = test_items.select(col("order_id"), col("product_id"), col("total_price").cast("double"), col("total_shipping_cost").cast("double"), col("total_cost").cast("double"), col("shipping_cost%").cast("double"), col("max_order_item_id").cast("double"), col("num_unique_products_per_id").cast("double"))

items.show(5)
test_items.show(5)

# COMMAND ----------

items.describe().show()
test_items.describe().show()

# COMMAND ----------

# DBTITLE 1,Table: order_payments & test_order_payments
"""
Metadata description: 
  •	order_id: Order unique identifier.
  •	payment_sequential: Sequential number identifying the order of the ordered items. A customer can order multiple items per order.
  •	payment_type: Method of payment chosen by the customer. 
  •	payment_installments: Number of installments chosen by the customer. 
  •	payment_value: Order value in euro.
"""

# COMMAND ----------

#Read table: order_payments 

payments =spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(payments_path)

payments.show(3)
payments.printSchema()
payments.count()
payments.dropDuplicates()

#Read table: test_order_payments 

test_payments =spark\
.read\
.format("csv")\
.option("header","true")\
.option("inferSchema","true")\
.option("escape","\"")\
.load(test_payments_path)

test_payments.show(3)
test_payments.printSchema()
test_payments.count()
test_payments.dropDuplicates()

# COMMAND ----------

#Count the number of nulls per column for order_payments 
payments.select([count(when(col(c).isNull(), c)).alias(c) for c in payments.columns]).show()
#Count the number of nulls per column for test_order_payments 
test_payments.select([count(when(col(c).isNull(), c)).alias(c) for c in test_payments.columns]).show()

# COMMAND ----------

# new variables base on payment method 
paymentsnv = payments.groupBy("order_id", "payment_type") \
    .agg(sum(col("payment_value")).alias("total_payment_value"))
pivoted_df = paymentsnv.groupBy("order_id").pivot("payment_type").agg(sum("total_payment_value").alias("total_payment_value"))
pivoted_df = pivoted_df.na.fill(0)
payments = payments.join(pivoted_df, on = "order_id")

test_payments = test_payments.where(test_payments['payment_type']!= "not_defined")

testpaymentnv = test_payments.groupBy("order_id", "payment_type") \
    .agg(sum(col("payment_value")).alias("total_payment_value"))
pivoted_tp = testpaymentnv.groupBy("order_id").pivot("payment_type").agg(sum("total_payment_value").alias("total_payment_value"))
pivoted_tp = pivoted_tp.na.fill(0)
test_payments = test_payments.join(pivoted_tp, on = "order_id")

# If customer choose to pay with installments 
payments = payments.withColumn("pay_with_installment", (col("payment_installments") > 1).cast("double"))
test_payments = test_payments.withColumn("pay_with_installment", (col("payment_installments") > 1).cast("double"))

# Remove duplicate or useless columns 
payments = payments.drop("payment_sequential","payment_type","payment_installments","payment_value")
test_payments = test_payments.drop("payment_sequential","payment_type","payment_installments","payment_value")

payments.show(3)
test_payments.show(3)

# COMMAND ----------

# DBTITLE 1,Table: order_reviews
"""
Metadata description: 
  •	review_id Unique identifier for a review
  •	order_id Order unique identifier
  •	review_score Score from 1 to 5 given by the customer on a customer satisfaction survey
  •	review_creation_date Date at which the customer satisfaction survey was sent to the customer.
  •	review_answer_timestamp Timestamp at which the customer answered to the customer satisfaction survey
"""

# COMMAND ----------

#Read table: orders_reviews
orders_reviews = spark.read.format("csv").option("header", "true").option("multiline", "true").load(order_review_path)
orders_reviews.show(5)
orders_reviews.printSchema()
orders_reviews.describe()

#Count the number of nulls per column
orders_reviews.describe().show()
from pyspark.sql.functions import isnan, when, count, col
orders_reviews.select([count(when(col(c).isNull(), c)).alias(c) for c in orders_reviews.columns]).show()

# COMMAND ----------


# create dummy for good and bad review 
orders_reviews = orders_reviews.withColumn("Target", when(col("review_score")>=4, 1).otherwise(0))

orders_reviews = orders_reviews.select(col("review_id"), col("order_id"), col("review_score").cast("double"), col("Target").cast("double"))

orders_reviews.show(5)

# COMMAND ----------

# DBTITLE 1,Create basetable by tables above
# Create TrainingSet with order data 
TrainingSet = items.join(payments, on='order_id')
TrainingSet = TrainingSet.join(product, on='product_id')
TrainingSet = TrainingSet.join(orders, on='order_id')  
TrainingSet = TrainingSet.join(orders_reviews, on = "order_id")   
TrainingSet.show(3)

# Create TestSet with test data 
TestSet = test_items.join(test_payments, on= 'order_id')
TestSet = TestSet.join(test_product,on= 'product_id')
TestSet = TestSet.join(test_orders, on = 'order_id')
TestSet.show(3)

# COMMAND ----------

#Count the number of nulls per column for order_payments 
TrainingSet.select([count(when(col(c).isNull(), c)).alias(c) for c in TrainingSet.columns]).show()
#Count the number of nulls per column for test_order_payments 
TestSet.select([count(when(col(c).isNull(), c)).alias(c) for c in TestSet.columns]).show()

# COMMAND ----------

# new variables in basetable 
# shipping cost per kg
TrainingSet = TrainingSet.withColumn("shipping_cost/kg", round(col("total_shipping_cost")/col("weight_kg"),2))
TestSet = TestSet.withColumn("shipping_cost/kg", round(col("total_shipping_cost")/col("weight_kg"),2))

# New variables related to product table for TrainingSet
Trainnv = TrainingSet.groupBy("order_id").agg(
    sum(col("weight_kg")).alias("ttl_weight"),
    sum(col("product_name_lenght")).alias("ttl_name"),
    sum(col("Prod_volume_m3")).alias("ttl_volume"),
    sum(col("product_photos_qty")).alias("ttl_photo"),
    sum(col("product_description_lenght")).alias("ttl_description"),
    avg(col("product_name_lenght")).alias("mean_name"),
    avg(col("product_photos_qty")).alias("mean_photo"),
    avg(col("product_description_lenght")).alias("mean_description"),
    (sum(col("weight_kg"))/ sum(col("Prod_volume_m3"))).alias("weight_volume"),
    round(sum(col("product_length")) / sum(col("product_width")), 2).alias("aspect_ratio_length_width"),
    round(sum(col("product_height")) / sum(col("product_width")), 2).alias("aspect_ratio_height_width"),
    round(sum(col("product_photos_qty")) / sum(col("product_description_lenght")), 2).alias("photo_description_ratio")
)

# Join Trainnv with TrainingSet
TrainingSet = TrainingSet.join(Trainnv, on="order_id").drop(
    "weight_kg", "Prod_volume_m3", "product_photos_qty", "product_description_lenght",
    "product_name_lenght", "product_length", "product_height", "product_width"
)

# New variables related to product table for TestSet
testnv = TestSet.groupBy("order_id").agg(
    sum(col("weight_kg")).alias("ttl_weight"),
    sum(col("product_name_lenght")).alias("ttl_name"),
    sum(col("Prod_volume_m3")).alias("ttl_volume"),
    sum(col("product_photos_qty")).alias("ttl_photo"),
    sum(col("product_description_lenght")).alias("ttl_description"),
    avg(col("product_name_lenght")).alias("mean_name"),
    avg(col("product_photos_qty")).alias("mean_photo"),
    avg(col("product_description_lenght")).alias("mean_description"),
    (sum(col("weight_kg"))/ sum(col("Prod_volume_m3"))).alias("weight_volume"),
    round(sum(col("product_length")) / sum(col("product_width")), 2).alias("aspect_ratio_length_width"),
    round(sum(col("product_height")) / sum(col("product_width")), 2).alias("aspect_ratio_height_width"),
    round(sum(col("product_photos_qty")) / sum(col("product_description_lenght")), 2).alias("photo_description_ratio")
)

TestSet = TestSet.join(testnv, on = "order_id").drop("weight_kg", "Prod_volume_m3","product_photos_qty","product_description_lenght","product_name_lenght","product_length","product_height","product_width")
                                    

# COMMAND ----------

# Drop duplicates in the TrainingSet and TestSet
TrainingSet = TrainingSet.drop_duplicates(subset=['order_id'])
TestSet = TestSet.drop_duplicates(subset=['order_id'])

TrainingSet = TrainingSet.dropna()
TestSet = TestSet.dropna()

TrainingSet.select("order_id").distinct().count() # 48781
TrainingSet.count() #48781
TestSet.count()

# COMMAND ----------

# Define the number of photos for the products (small, okay, good) 
not_enough = 5
okay = 15
TrainingSet = TrainingSet.withColumn(
    "nbr_photo",
    when(col("mean_photo") <= not_enough, "Minimal")
    .when((col("mean_photo") >= not_enough) & (col("mean_photo") <= okay), "Moderate")
    .otherwise("Abundant")
)
TestSet = TestSet.withColumn(
   "nbr_photo",
    when(col("mean_photo") <= not_enough, "Minimal")
    .when((col("mean_photo") >= not_enough) & (col("mean_photo") <= okay), "Moderate")
    .otherwise("Abundant")
)

# Name complexity: To categorizes products according to the length of the names []
short_threshold =20
medium_threshold = 50
TrainingSet = TrainingSet.withColumn(
    "name_length",
    when(col("mean_name") <= short_threshold, "Short name")
    .when((col("mean_name")>= short_threshold) & (col("mean_name") <= medium_threshold), "Medium name")
    .otherwise("Long name")
)

TestSet = TestSet.withColumn(
    "name_length",
    when(col("mean_name") <= short_threshold, "Short name")
    .when((col("mean_name")>= short_threshold) & (col("mean_name") <= medium_threshold), "Medium name")
    .otherwise("Long name")
)

# Define the size of the product description (Short, Medium or long description)
short_description = 500
medium_description = 1500
TrainingSet = TrainingSet.withColumn(
    "description_length",
    when(col("mean_description") <= short_description, "Short description")
    .when((col("mean_description") >= short_description) & (col("mean_description") <= medium_description), "Medium description")
    .otherwise("Long description")
)
TestSet = TestSet.withColumn(
    "description_length",
    when(col("mean_description") <= short_description, "Short description")
    .when((col("mean_description") >= short_description) & (col("mean_description") <= medium_description), "Medium description")
    .otherwise("Long description")
)

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline
# Create Dummy variables 
TrainingSet = Pipeline(stages=[
    StringIndexer(inputCols=[ "name_length", "description_length", "nbr_photo"], outputCols=["name_lengthInd", "prod_desInd", "nbr_photoInd"]),
    OneHotEncoder(inputCols=[ "name_lengthInd", "prod_desInd", "nbr_photoInd"], outputCols=["name_length_dum", "prod_desc_dum", "nbr_photo_dum"])
]).fit(TrainingSet).transform(TrainingSet)

TestSet = Pipeline(stages=[
    StringIndexer(inputCols=[ "name_length", "description_length", "nbr_photo"], outputCols=["name_lengthInd", "prod_desInd", "nbr_photoInd"]),
    OneHotEncoder(inputCols=["name_lengthInd", "prod_desInd", "nbr_photoInd"], outputCols=["name_length_dum", "prod_desc_dum", "nbr_photo_dum"])
]).fit(TestSet).transform(TestSet)


# COMMAND ----------

# Create dummy variables for training set
category_name_train = TrainingSet.groupBy("order_id", "product_category_name")
pivoted_train = category_name_train.pivot("product_category_name").agg(count("product_category_name"))
pivoted_train = pivoted_train.na.fill(0)
TrainingSet = TrainingSet.join(pivoted_train, on="order_id")

# Create dummy variables for test set
category_name_test = TestSet.groupBy("order_id", "product_category_name")
pivoted_test = category_name_test.pivot("product_category_name").agg(count("product_category_name"))
pivoted_test = pivoted_test.na.fill(0)
TestSet = TestSet.join(pivoted_test, on="order_id")

# COMMAND ----------

TrainingSet = TrainingSet.withColumn("ttl_name", col("ttl_name").cast("double"))
TrainingSet = TrainingSet.withColumn("ttl_description",col('ttl_description').cast("double"))
TrainingSet = TrainingSet.withColumn("ttl_photo",col('ttl_photo').cast("double"))
TestSet = TestSet.withColumn("ttl_name", col("ttl_name").cast("double"))
TestSet = TestSet.withColumn("ttl_description",col('ttl_description').cast("double"))
TestSet = TestSet.withColumn("ttl_photo",col('ttl_photo').cast("double"))

# COMMAND ----------

# Drop unnecessary columns
TrainingSet = TrainingSet.drop("product_category_name","name_lengthInd", "prod_desInd", "nbr_photoInd","name_length", "description_length", "nbr_photo")
TestSet = TestSet.drop("product_category_name","name_lengthInd", "prod_desInd", "nbr_photoInd","name_length", "description_length", "nbr_photo")

#Count the number of nulls per column for order_payments 
TrainingSet.select([count(when(col(c).isNull(), c)).alias(c) for c in TrainingSet.columns]).show()
#Count the number of nulls per column for test_order_payments 
TestSet.select([count(when(col(c).isNull(), c)).alias(c) for c in TestSet.columns]).show()

# COMMAND ----------

display(TrainingSet)

# COMMAND ----------

display(TestSet)

# COMMAND ----------

# MAGIC %md
# MAGIC ####----- Part 2: Modeling -----

# COMMAND ----------

TrainingSet = spark.read.format("csv").option("header", "True").option("inferSchema", "True").option("escape", "\"").load("/FileStore/TrainingSet.csv")
TestSet = spark.read.format("csv").option("header", "True").option("inferSchema", "True").option("escape", "\"").load("/FileStore/TestSet.csv")

# COMMAND ----------

from scipy.stats import pearsonr

# Columns to exclude (also exclude dummies, seems can't know the p-value)
columns_to_exclude = ["order_id", "product_id", "review_id", "customer_id", "name_length_dum", "prod_desc_dum", "nbr_photo_dum","scaledNumericalFeatures"]
target_column = 'Target'
columns_selected = [column for column in TrainingSet.columns if column != target_column and column not in columns_to_exclude]

# Define the significance threshold
significance_threshold = 0.001

# Initialize a list to store selected features
selected_features = []

# Calculate Pearson correlation and p-value for each feature
for column in columns_selected:
    if column != target_column:
        pearson_corr = TrainingSet.stat.corr(column, target_column)
        feature_column = TrainingSet.select(column).rdd.flatMap(lambda x: [float(i) for i in x]).collect()
        label_column = TrainingSet.select(target_column).rdd.flatMap(lambda x: [float(i) for i in x]).collect()
        _, p_value = pearsonr(feature_column, label_column)
        print(f"{column} - p-value = {p_value} - selected: {1 if p_value < significance_threshold else 0}")
        if p_value < significance_threshold:
            selected_features.append(column)

# Print the selected features
print("\nSelected Features:", selected_features)

# COMMAND ----------

Selected_Features = ['total_price', 'total_shipping_cost', 'total_cost', 'shipping_cost%', 'max_order_item_id', 'num_unique_products_per_id', 'credit_card', 'mobile', 'pay_with_installment', 'approve_efficiency', 'package_efficiency', 'delivery_efficiency', 'on_time', 'total_delivery_time', 'shipping_cost/kg', 'ttl_weight', 'ttl_name', 'ttl_volume', 'ttl_photo', 'ttl_description', 'mean_name', 'mean_description', 'audio', 'bed_bath_table', 'books_general_interest', 'cool_stuff', 'furniture_decor', 'luggage_accessories', 'office_furniture', 'sports_leisure', 'stationery', 'toys']
Features_and_label = Selected_Features + ["review_score","Target"]
Features_and_label_Test = Selected_Features + ["order_id"]
dTrain = TrainingSet.select(*Features_and_label)
dTest = TestSet.select(*Features_and_label_Test)


# COMMAND ----------

from pyspark.ml.feature import RFormula
train = RFormula(formula="Target ~ . -review_score - order_id").fit(dTrain).transform(dTrain)
test = RFormula(formula="Target ~ . - review_score - order_id").fit(dTest).transform(dTest)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

# VectorAssembler to combine numerical variables into a single vector column
assembler = VectorAssembler(inputCols=Selected_Features, outputCol="numericalFeatures")
assembled_Train = assembler.transform(train)
assembled_Test = assembler.transform(test)
# StandardScaler to scale the numerical features
scaler = StandardScaler(inputCol="numericalFeatures", outputCol="scaleFeatures", withStd=True, withMean=False)
scaler_model = scaler.fit(assembled_Train)
scaler_model_test = scaler.fit(assembled_Test)
train = scaler_model.transform(assembled_Train).drop("numericalFeatures")
test = scaler_model_test.transform(assembled_Test).drop("numericalFeatures")


# COMMAND ----------


rTrain, rValidation= train.randomSplit([0.7,0.3], seed=123)


# COMMAND ----------

from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator

gbt_model = GBTClassifier(featuresCol="scaleFeatures", labelCol="Target").fit(rTrain)

gbt_pred = gbt_model.transform(rValidation)

AUC_gbt = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(gbt_pred)
Precision_gbt = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(gbt_pred)
Recall_gbt = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(gbt_pred)
f1_gbt = MulticlassClassificationEvaluator(metricName="f1").evaluate(gbt_pred)

print("AUC:", AUC_gbt)
print("Precision:", Precision_gbt)
print("Recall:", Recall_gbt)
print("F1 Score:", f1_gbt)


# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol="scaleFeatures", labelCol="Target")

cvlr = CrossValidator()\
  .setEstimator(lr)\
  .setEstimatorParamMaps(ParamGridBuilder().addGrid(lr.regParam, [0.1, 0.01]).addGrid(lr.maxIter, [50, 100, 150]).build())\
  .setEvaluator(BinaryClassificationEvaluator())\
  .setNumFolds(5)

cvlrModel = cvlr.fit(rTrain)

# Get parameters that had the best performance
print("Best LR model:")  
print("** regParam: " + str(cvlrModel.bestModel.getRegParam()))
print("** maxIter: " + str(cvlrModel.bestModel.getMaxIter()))

# COMMAND ----------

lr_preds = cvlrModel.transform(rValidation)

AUC_lr = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(lr_preds)
Precision_lr = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(lr_preds)
Recall_lr = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(lr_preds)
f1_lr = MulticlassClassificationEvaluator(metricName="f1").evaluate(lr_preds)

print("AUC :", AUC_lr)
print("Precision :", Precision_lr)
print("Recall :", Recall_lr)
print("F1_score :", f1_lr)

# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Assuming you have a RandomForestClassifier
rf = RandomForestClassifier(featuresCol="scaleFeatures", labelCol="Target")

cv_rf = CrossValidator()\
  .setEstimator(rf)\
  .setEstimatorParamMaps(ParamGridBuilder().addGrid(rf.maxDepth, [5, 10]).addGrid(rf.numTrees, [20, 50, 100]).build())\
  .setEvaluator(BinaryClassificationEvaluator())\
  .setNumFolds(5)

cv_rf_model = cv_rf.fit(rTrain)

# COMMAND ----------

rf_pred = cv_rf_model.transform(rValidation)

AUC_rf = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(rf_pred)
Precision_rf = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(rf_pred)
Recall_rf = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(rf_pred)
f1_rf = MulticlassClassificationEvaluator(metricName="f1").evaluate(rf_pred)

print("AUC :", AUC_rf)
print("Precision :", Precision_rf)
print("Recall :", Recall_rf)
print("F1_score :", f1_rf)

# COMMAND ----------

from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Assuming you have a LinearSVC model
svm = LinearSVC(featuresCol="scaleFeatures", labelCol="Target", standardization=False, maxIter=10)

cvsvm = CrossValidator()\
  .setEstimator(svm)\
  .setEstimatorParamMaps(ParamGridBuilder().addGrid(svm.regParam, [0.1, 0.01]).addGrid(svm.maxIter, [50, 100, 150]).build())\
  .setEvaluator(BinaryClassificationEvaluator())\
  .setNumFolds(5)

cvsvmModel = cvsvm.fit(rTrain)

# COMMAND ----------

svm_pred = cvsvmModel.transform(rValidation)

AUC_svm = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(svm_pred)
Precision_svm = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(svm_pred)
Recall_svm = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(svm_pred)
f1_svm = MulticlassClassificationEvaluator(metricName="f1").evaluate(svm_pred)

print("AUC :", AUC_svm)
print("Precision :", Precision_svm)
print("Recall :", Recall_svm)
print("F1_score :", f1_svm)

# COMMAND ----------

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

# Assuming you have already prepared your training data in 'rTrain'
dt = DecisionTreeClassifier(featuresCol="scaleFeatures", labelCol="Target", maxDepth=5)

# Create a CrossValidator
cvdt = CrossValidator() \
    .setEstimator(dt) \
    .setEstimatorParamMaps(ParamGridBuilder().addGrid(dt.maxDepth, [5, 10, 15]).build()) \
    .setEvaluator(BinaryClassificationEvaluator()) \
    .setNumFolds(5)

# Fit the model
cvdtModel = cvdt.fit(rTrain)

# COMMAND ----------

dt_pred = cvdtModel.transform(rValidation)

AUC_dt = BinaryClassificationEvaluator(metricName="areaUnderROC").evaluate(dt_pred)
Precision_dt = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(dt_pred)
Recall_dt = MulticlassClassificationEvaluator(metricName="weightedRecall").evaluate(dt_pred)
f1_dt = MulticlassClassificationEvaluator(metricName="f1").evaluate(dt_pred)

print("AUC :", AUC_dt)
print("Precision :", Precision_dt)
print("Recall :", Recall_dt)
print("F1_score :", f1_dt)

# COMMAND ----------

# MAGIC %md
# MAGIC ----- Multi-Class Classification -----

# COMMAND ----------

# DBTITLE 1,multi-class classification
from scipy.stats import pearsonr

# Columns to exclude (also exclude dummies)
columns_to_exclude = ["order_id", "product_id", "review_id", "customer_id", "name_length_dum", "prod_desc_dum", "nbr_photo_dum", "Target", "scaledNumericalFeatures"]
target_column = 'review_score'
columns_selected = [column for column in TrainingSet.columns if column != target_column and column not in columns_to_exclude]

# Define the significance threshold
significance_threshold = 0.001

# Initialize a list to store selected features
selected_features = []

# Calculate Pearson correlation and p-value for each feature
for column in columns_selected:
    if column != target_column:
        feature_column = TrainingSet.select(column).rdd.flatMap(lambda x: [float(i) for i in x]).collect()
        label_column = TrainingSet.select(target_column).rdd.flatMap(lambda x: [float(i) for i in x]).collect()

        # Handle NaN in correlation
        try:
            pearson_corr = TrainingSet.stat.corr(column, target_column)
        except:
            pearson_corr = 0.0  # Set to 0 if correlation is NaN

        _, p_value = pearsonr(feature_column, label_column)
        print(f"{column} - Correlation = {pearson_corr:.4f}, p-value = {p_value:.4f} - selected: {1 if p_value < significance_threshold else 0}")
        
        if p_value < significance_threshold:
            selected_features.append(column)

# Print the selected features
print("\nSelected Features:", selected_features)


# COMMAND ----------

Features = ['total_price', 'total_shipping_cost', 'total_cost', 'shipping_cost%', 'max_order_item_id', 'num_unique_products_per_id', 'credit_card', 'mobile', 'pay_with_installment', 'approve_efficiency', 'package_efficiency', 'delivery_efficiency', 'on_time', 'total_delivery_time', 'shipping_cost/kg', 'ttl_weight', 'ttl_name', 'ttl_volume', 'ttl_photo', 'ttl_description', 'mean_name', 'aspect_ratio_length_width', 'bed_bath_table', 'books_general_interest', 'computers_accessories', 'cool_stuff', 'fashion_male_clothing', 'furniture_decor', 'health_beauty', 'luggage_accessories', 'office_furniture', 'perfumery', 'sports_leisure', 'toys']
Features_label = Features + ["review_score","order_id"]
Features_label_test = Features + ["order_id"]
mcTrain = TrainingSet.select(*Features_label)
mcTest = TestSet.select(*Features_label_test)

# COMMAND ----------

from pyspark.ml.feature import RFormula
mtrain = RFormula(formula="review_score ~ . - order_id").fit(mcTrain).transform(mcTrain)
mtest = RFormula(formula="review_score ~ . -order_id").fit(mcTest).transform(mcTest)

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

# VectorAssembler to combine numerical variables into a single vector column
assembler = VectorAssembler(inputCols=Selected_Features, outputCol="numericalFeatures")
assembled_Train = assembler.transform(mtrain)

# StandardScaler to scale the numerical features
scaler = StandardScaler(inputCol="numericalFeatures", outputCol="scaleFeatures", withStd=True, withMean=False)
scaler_model = scaler.fit(assembled_Train)
mtrain = scaler_model.transform(assembled_Train).drop("numericalFeatures")

# COMMAND ----------


mTrain, mValidation = mtrain.randomSplit([0.7, 0.3],seed=123)

# COMMAND ----------

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

mlr = LogisticRegression(featuresCol="scaleFeatures", labelCol="review_score", family="multinomial")

cvmlr = CrossValidator()\
  .setEstimator(mlr)\
  .setEstimatorParamMaps(ParamGridBuilder().addGrid(mlr.regParam, [0.1, 0.01]).addGrid(mlr.maxIter, [50, 100, 150]).build())\
  .setEvaluator(MulticlassClassificationEvaluator())\
  .setNumFolds(5)

cvmlrModel = cvmlr.fit(mTrain)

# COMMAND ----------

mlr_pred = cvmlrModel.transform(mValidation)

accuracy_mlr = MulticlassClassificationEvaluator(metricName='accuracy').evaluate(mlogreg_pred)
weightedPrecision_mlr = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(mlogreg_pred)
weightedRecall_mlr = MulticlassClassificationEvaluator(metricName='weightedRecall').evaluate(mlogreg_pred)
f1_mlr = MulticlassClassificationEvaluator(metricName='f1').evaluate(mlogreg_pred)

print("Accuracy:", accuracy_mlr)
print("Weighted Precision:", weightedPrecision_mlr)
print("Weighted Recall:", weightedRecall_mlr)
print("F1 Score:", f1_mlr)


# COMMAND ----------

from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# Assuming 'review_score' is your label column
mrf = RandomForestClassifier(featuresCol="scaleFeatures", labelCol="review_score")

cv_mrf = CrossValidator()\
  .setEstimator(mrf)\
  .setEstimatorParamMaps(ParamGridBuilder().addGrid(mrf.maxDepth, [5, 10]).addGrid(mrf.numTrees, [20, 50, 100]).build())\
  .setEvaluator(MulticlassClassificationEvaluator())\
  .setNumFolds(5)

cv_mrf_model = cv_mrf.fit(mTrain)


# COMMAND ----------

mrf_pred = cv_mrf_model.transform(mValidation)

accuracy_mrf = MulticlassClassificationEvaluator(metricName='accuracy').evaluate(mrf_pred)
weightedPrecision_mrf = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(mrf_pred)
weightedRecall_mrf = MulticlassClassificationEvaluator(metricName='weightedRecall').evaluate(mrf_pred)
f1_mrf = MulticlassClassificationEvaluator(metricName='f1').evaluate(mrf_pred)

print("Accuracy:", accuracy_mrf)
print("Weighted Precision:", weightedPrecision_mrf)
print("Weighted Recall:", weightedRecall_mrf)
print("F1 Score:", f1_mrf)

# COMMAND ----------

# MAGIC %md
# MAGIC ---- Prediction ----

# COMMAND ----------

output_pred = cv_rf_model.transform(test)
output_pred = output_pred.withColumnRenamed("prediction","pred_review_score")
display(output_pred.select("order_id", "pred_review_score"))



# COMMAND ----------

output_pred.groupBy("pred_review_score").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---- Thank you for your reading ----
