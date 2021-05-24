
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from datetime import timedelta, datetime
from textwrap import dedent
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from azure.storage.blob import BlobServiceClient
from io import BytesIO

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *




                

accountname = "aglakewest2"
accountkey = "TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg=="
containername = "ingress"
pathname = "data/adventureworks/ods/"
foldername = "customer/"
filename = "Customer2.parquet"
connect_str = "DefaultEndpointsProtocol=https;AccountName=aglakewest2;AccountKey=TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg==;EndpointSuffix=core.windows.net"
blobname = pathname + foldername + "/" + filename            
current_datetime = datetime.now()
firmlist = ["raa"]


conf = SparkConf()
spark = SparkSession.builder.appName("send_file").getOrCreate()
#data = spark.read.option("header","true").csv("/usr/local/spark/app/Customer.csv")
with BytesIO() as input_blob:
	spark.sql("set spark.sql.streaming.schemaInference=true")
	customerSchema = StructType().add("CustomerID","string").add("NameStyle","string").add("Title","string").add("FirstName","string").add("MiddleName","string").add("LastName","string").add("Suffix","string").add("CompanyName","string").add("SalesPerson","string").add("EmailAddress","string").add("Phone","string").add("PasswordHash","string").add("PasswordSalt","string").add("rowguid","string").add("ModifiedDate","string")
	
	csv_input = spark.readStream.option("sep",",").option("header","true").schema(customerSchema).csv("/usr/local/spark/app/files")
	#csv_input.write.format("parquet").option("mode","overwrite").save("/usr/local/spark/app/Customertable")
	csv_input.writeStream.format("parquet").option("checkpointLocation", "/usr/local/spark/app/checkpoint").option("path","/usr/local/spark/app/Customertable").start()
	#with open("/usr/local/spark/app/Customer.parquet", mode='rb') as infile:
	#	input_blob =infile.read()
	#	blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=containername, blob_name=blobname)
	#	blob.upload_blob(input_blob, overwrite='true')


