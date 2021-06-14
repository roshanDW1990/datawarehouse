# Databricks notebook source
#connecting to my data lake where all teh weather file are stored.
#to note i would normally use secrets ie #print(dbutils.secrets.get(scope = "weatherScope", key = "storageaccount")), however i do not have permissions to create secrets using my work account.
storage_account_name = "weatherlake01"
storage_account_key = "o4J9U1F0p5+RgTSRRjCncRBYbU7lH5/HUWwdNGj9tV8H/dz1i0YXtrzRnbECR1dGbY+6V8YdQd6ertdrjRSviw=="
container = "deliverect"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

# COMMAND ----------

dbutils.fs.ls(f"wasbs://{container}@{storage_account_name}.blob.core.windows.net/")

# COMMAND ----------

#unmount any mount points to get a refreshed mount.  This mount will look at my weather files.
MOUNTPOINT = "/mnt/deliverect"

if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
  dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

##connect to the lake container
CONTAINER = "deliverect"

#as noted previously i would use secrets where possible, example below
#SASTOKEN = dbutils.secrets.get(scope="weatherScope", key="storagewrite")

SASTOKEN = "?sv=2020-02-10&ss=bfqt&srt=sco&sp=rwdlacupx&se=2021-07-10T22:30:50Z&st=2021-06-01T14:30:50Z&spr=https&sig=UaXBbUkBeYLeaaMQwDIrvKspN6EQ58XFl6%2FDjgVhNdU%3D"

# Redefine the source and URI for the new container
SOURCE = "wasbs://{container}@{storage_acct}.blob.core.windows.net/".format(container=CONTAINER, storage_acct=storage_account_name)
URI = "fs.azure.sas.{container}.{storage_acct}.blob.core.windows.net".format(container=CONTAINER, storage_acct=storage_account_name)
               
# Set up container SAS
spark.conf.set(URI, SASTOKEN)

try:
  dbutils.fs.mount(
    source=SOURCE,
    mount_point=MOUNTPOINT,
    extra_configs={URI:SASTOKEN})
except Exception as e:
  if "Directory already mounted" in str(e):
    pass # Ignore error if already mounted.
  else:
    raise e
print("Success.")

# COMMAND ----------

from datetime import datetime

# COMMAND ----------

now = datetime.now()
fileyear = now.strftime("%Y")
filemonth = now.strftime("%m")
fileday = now.strftime("%d")

fileDate = now.strftime("H%M%S")
clientFileCSV = "rawsales_cars"+fileDate

clientFileCSVyear = "Year = "+ str(fileyear)
clientFileCSVmonth = "Month = " + str(filemonth)
clientFileCSVday = "day = " + str(fileday)

path = str(clientFileCSVyear + '/' + clientFileCSVmonth + '/' + clientFileCSVday + '/')

# COMMAND ----------

##Paths where files are stored and will be saved
inputPathcore = "/mnt/deliverect/rawfiles/rawcore/*.csv"
inputPathplus = "/mnt/deliverect/rawfiles/rawplus/*.csv"

inputShop = "mnt/deliverect/rawfiles/onpremfiles/shop/*.csv" 
inputProduct = "mnt/deliverect/rawfiles/onpremfiles/*.csv"

dataPathcore = SOURCE + "/mart/core" 
dataPathplus = SOURCE + "/mart/plus" 

powerbi = SOURCE + "/rawfiles/powerbi"


# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#read raw file frim the landing folder
#######NOTE IF PATH FAILS IS BECAUSE THE sas token has expired, so need to go to the storage account in azure and reassign the SAS token and post it to secrest#####
coredf = (spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(inputPathcore))

##newcoredf = coredf.withColumn("delta_timestamp", current_date())

##newcoredf.write.mode("overwrite").format("delta").save(dataPathcore)

plusdf = (spark.read
            .option("header", True)
            .option("inferschema", True)
            .csv(inputPathplus))


shopdf = (spark.read
         .option("header", True)
         .option("inferschema", True)
         .csv(inputShop))

productdf = (spark.read
         .option("header", True)
         .option("inferschema", True)
         .csv(inputProduct))

##newplusdf = plusdf.withColumn("delta_timestamp", current_date())

##newplusdf.write.mode("overwrite").format("delta").save(dataPathplus)


# COMMAND ----------


#select plus columns
plusfilterdf = plusdf.select(col('Order_ID').alias('order_id'), col('Plu').alias('plu'), col('Quantity').alias('quantity'), col('Name').alias('name'))

display(plusfilterdf)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

#select core columns
corefilterdf = coredf.select(col('Order_ID').alias('order_id'), col('Pickuptime_Time').alias('pickup_time'), col('load_date'))
display(corefilterdf)

# COMMAND ----------

# Create a view or table for all df's

temp_table_plus = "sqlplus"
temp_table_core = "sqlcore"
temp_table_shop = "sqlshop"
temp_table_product = "sqlproduct"


plusfilterdf.createOrReplaceTempView(temp_table_plus)
corefilterdf.createOrReplaceTempView(temp_table_core)

shopdf.createOrReplaceTempView(temp_table_shop)
productdf.createOrReplaceTempView(temp_table_product)

# COMMAND ----------

dbutils.widgets.help()


# COMMAND ----------

#widget setup for parameters
dbutils.widgets.text(name = "begindate", defaultValue = "YYYY-MM-DD", label = "begindate")
dbutils.widgets.get("begindate")
y = getArgument("begindate")

print (y)

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS deliverect

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS deliverect.report_data;
# MAGIC 
# MAGIC CREATE TABLE deliverect.report_data
# MAGIC USING delta
# MAGIC 
# MAGIC SELECT *  
# MAGIC FROM sqlcore
# MAGIC WHERE pickup_time >= getArgument("begindate") 

# COMMAND ----------

datadf = sqlContext.sql("""Select * from deliverect.report_data order by load_date desc""")

display(datadf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from deliverect.report_data

# COMMAND ----------

datadf.write.format('parquet').option("header", True).mode("overwrite").save(powerbi)

# COMMAND ----------

#from pyspark.sql.types import *

# COMMAND ----------

#create scheman tables
#coreSchema = StructType (
#[
#  StructField("order_id", StringType(), True),
#  StructField("pickuptime_time", StringType(), True)
#]
#) 

#plusSchema = StructType (
#[
#  StructField("quantity",StringType(),True)
  
#]

#)

# COMMAND ----------

