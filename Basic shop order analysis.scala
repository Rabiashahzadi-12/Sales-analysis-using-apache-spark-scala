import org.apache.spark.sql.SparkSession
var spark =SparkSession.builder.appName(“Read CSV file”).getOrCreate()
var df = spark.read.csv(path of dataset)
df.show()
val Rename_df = df.withColumnRenamed(“_c0”,”OrderID”).withColumnRenamed(“_c1”,”Product”).withColumnRenamed(“_c2”,”Quantity”).withColumnRenamed(“_c3”,”Timestamp”)
Rename_df.show()
val drop_df = Rename_df.filter(“OrderID !=0”)
drop_df.show()
val sorted_df = drop_df.withColumn(“OrderID”,drop_df(“OrderID”).cast(“int”)).orderBy(“OrderID”)
sorted_df.show()

var total_orders=sorted_df.count()
print("Total orders:",total_orders)

import org.apache.spark.sql.functions.{max, desc}
var spark1 = SparkSession.builder.appName(“Most popular product”).getOrCreate()
val productQuantityDF =  sort_df.groupBy(“Product”).agg(sum(“Quantity”).alias(“total_quantity”)).orderBy(desc(“total_quantity”))
productQunatityDF.show(1)
productQuantityDF.show(2)

import org.apache.spark.sql.functions.{col,month}
var date = df.groupBy(to_date(col(“_c3”)).alias(avg(“_c2”).alias(“average_order_quantity”))
val month_df = date.withColumn(“Month”,month(col(“Timestamp”)))
month_df.show()
var orderbymonth = group_df.groupBy(to_date(col(“Timestamp”)).alias(“month”)).agg(avg(“Quantity”).alias(“average_order_quantity”))
orderbymonth.show()

print("========Report=========")
printIn("Total number of orders:$total_orders")
printIn("$productQuantityDF")
printIn("Average order quantity by month:$orderbymonth")
 


