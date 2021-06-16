# Databricks notebook source
storage_account_name = "weatherlake01"
storage_account_key = "o4J9U1F0p5+RgTSRRjCncRBYbU7lH5/HUWwdNGj9tV8H/dz1i0YXtrzRnbECR1dGbY+6V8YdQd6ertdrjRSviw=="
container = "weather"

# COMMAND ----------

#unmount any mount points to get a refreshed mount.  This mount will look at my weather files.
MOUNTPOINT = "/mnt/weather"

if MOUNTPOINT in [mnt.mountPoint for mnt in dbutils.fs.mounts()]:
  dbutils.fs.unmount(MOUNTPOINT)

# COMMAND ----------

##connect to the lake container
CONTAINER = "weather"

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
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import *

# COMMAND ----------

##file paths with time

now = datetime.now()
fileyear = now.strftime("%Y")
filemonth = now.strftime("%m")
fileweek = now.strftime("%w")

msfileCSVyear = "Year = "+ str(fileyear)
msfileCSVmonth = "Month ="+ str(filemonth)
msfileCSVweek = "Week = "+ str(fileweek)

datepath = str(msfileCSVyear + "/" + msfileCSVmonth + "/" + msfileCSVweek)


# COMMAND ----------

#get data and store final results
getpath = SOURCE + "/DW_MS_files/*.csv"
storepath = SOURCE + "/DW_MS_files/MS_final/" + datapath


# COMMAND ----------

