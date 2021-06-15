# Databricks notebook source
storage_account_name = "weatherlake01"
storage_account_key = "o4J9U1F0p5+RgTSRRjCncRBYbU7lH5/HUWwdNGj9tV8H/dz1i0YXtrzRnbECR1dGbY+6V8YdQd6ertdrjRSviw=="
container = "deliverect"

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

dbutils.widgets.text("fileName", "","")

fileName = "/mnt/deliverect/" + dbutils.widgets.get("fileName")

df =  spark.read.option("header", True).option("inferschema", True).csv(fileName)

df.show()


# COMMAND ----------

new_df = df.withColumn("Yearsten", df["Years"] * 10)

display(new_df)

# COMMAND ----------

#save path in lake
storepath = "/mnt/deliverect/baseball_final"

# COMMAND ----------

#write to lake

new_df.write.format('parquet').option("header", True).mode("overwrite"delta").save(storepath)

#new_df.write.mode("overwrite").format("delta").save(storepath)

# COMMAND ----------

#dbutils.widgets.help()

# COMMAND ----------

#dbutils.widgets.removeAll()

# COMMAND ----------

