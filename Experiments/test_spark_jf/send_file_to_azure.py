
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




                

accountname = "aglakewest2"
accountkey = "TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg=="
containername = "ingress"
pathname = "data/adventureworks/ods/"
foldername = "customer/"
filename = "Customer.csv"
connect_str = "DefaultEndpointsProtocol=https;AccountName=aglakewest2;AccountKey=TVFVirCZKGRuuHPRAMZ1laEtUsgU3wrXQIRCehBCMC+xajNrDvfJ96GXskIC+zNiCfSd4mDDoaoOpmZd1YwXNg==;EndpointSuffix=core.windows.net"
blobname = pathname + foldername + "/" + filename            
current_datetime = datetime.now()
firmlist = ["raa"]


conf = SparkConf()
spark = SparkSession.builder.appName("send_file").getOrCreate()
#data = spark.read.option("header","true").csv("/usr/local/spark/app/Customer.csv")
spark.sql("set spark.sql.streaming.schemaInference=true")
data = spark.read.option("sep",",").option("header","true").csv("/usr/local/spark/app/Customer.csv")
#block_blob_service = BlockBlobService(account_name=accountname, account_key=accountkey)
#block_blob_service.create_blob_from_stream(containername,blobname,data)
blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=containername, blob_name=blobname)
blob.upload_blob("/usr/local/spark/app/Customer.csv", overwrite='true')




