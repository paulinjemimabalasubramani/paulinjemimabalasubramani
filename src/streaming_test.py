"""
Spark Streaming Test

"""

# %%

import os, sys, json, tempfile, shutil
sys.parent_name = os.path.basename(__file__)
sys.domain_name = 'client_account'
sys.domain_abbr = 'CA'

# Add 'modules' path to the system environment - adjust or remove this as necessary
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../../src'))
sys.path.append(os.path.realpath(os.path.dirname(__file__)+'/../src'))

from modules.common_functions import logger, catch_error, is_pc, data_settings, config_path, execution_date, strftime, mark_execution_end
from modules.spark_functions import create_spark, read_csv
from pyspark.sql.functions import col, lit
from pyspark.sql import functions as F


# %% Create Spark Session

spark = create_spark()


# %% Read CSV File

file_path = r'C:\packages\Shared\streaming_test\PS_20174392719_1491204439457_log.csv'

df = read_csv(spark=spark, file_path=file_path)

df = df.drop('isFraud', 'isFlaggedFraud')


# %%

df.groupBy('step').count().show(5)


# %%

source_path = r'C:\packages\Shared\streaming_test\stream_source1'

steps = df.select('step').distinct().collect()

for step in steps[:5]:
    print(f'Writing Step: {step[0]}')
    _df = df.where(col('step')==lit(step[0]))
    _df.coalesce(1).write.mode('append').option('header', 'true').csv(source_path)



# %%

streaming = (spark
    .readStream
    .schema(df.schema)
    .option('MaxFilesPerTrigger', 1)
    .csv(source_path)
    )




# %%

dest_count = streaming #.groupBy('nameDest').count().orderBy(F.desc('count'))

# %%

activityQuery = (
    dest_count.writeStream.queryName('dest_counts')
    .format('memory')
    .outputMode('append')
    .start()
    )


# %%

# activityQuery.awaitTermination()


# %%

for x in range(1):
    print(x)
    _df = spark.sql('select count(*) from dest_counts')
    if not _df.rdd.isEmpty():
        _df.show(10)


# %%

spark.streams.get(activityQuery.id).isActive


# %%
activityQuery.status

# %%
activityQuery.stop()


# %%
activityQuery.status

# %%

_="""

import net.snowflake.spark.snowflake.SnowflakeConnectorUtils
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.streaming.Trigger
import spark.implicits._
import org.apache.spark.sql.functions._

val inputStream = spark
.readStream
.format("kafka")
.option("kafka.bootstrap.servers", "localhost:9092")
.option("subscribe", "input_stream")
.option("startingOffsets", "earliest")
.load()
.selectExpr("CAST(value AS STRING)")

val jSchema = new StructType()
.add("id", IntegerType)
.add("name", StringType)
.add("fee", IntegerType)
.add("event_time", LongType)

val stream = inputStream.select(from_json(col("value"), jSchema).as("col1"))
.select(col("col1.*"))

val SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

SnowflakeConnectorUtils.enablePushdownSession(spark)

val sfOptions = new scala.collection.mutable.HashMap[String, String]()

sfOptions += (
"sfURL" -> "https://**********.privatelink.snowflakecomputing.com/",
"sfUser" -> "**********",
"sfPassword" -> "*********",
"sfDatabase" -> "UAT",
"sfSchema" -> "PUBLIC",
"sfWarehouse" -> "UAT_XSMALL_WH"
)


stream.writeStream.trigger(Trigger.ProcessingTime("30 seconds"))
.foreachBatch((ds, dt) => {
    ds.toDF().show(false)
    ds.toDF().write
    .format(SNOWFLAKE_SOURCE_NAME)
    .options(sfOptions)
    .option("dbtable", "members_stage")
    .mode(SaveMode.Append)
    .save()
    })
.start()
.awaitTermination()

"""

# %%

# %%

# %%
