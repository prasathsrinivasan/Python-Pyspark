Usecase 1 - End to End - Realtime CDC Data Pipeline:
1. Create a Nifi workflow with the processors such as QueryDatabaseTable -> ConvertAvroToJson -> PublishKafka_0_10
2. Create the database and weblog table  and ingest the data slowly changing it.

create database weblogs;
use weblogs;
create table click_stream(id int,custid int,ip varchar(100),updts timestamp,year int,visit_secs int,http_verb varchar(10),short_url varchar(100),long_url varchar(300),browser varchar(500),status_cd int,upddt date);

insert into click_stream values(1,1743,'68.688.326.58',current_timestamp(),2022,45,'GET','/products','/product/product4','Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Hewlett-Packard; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MS-RTC LM 8; InfoPath.2)',200,current_date());
insert into click_stream values(2,75756,'361.631.17.30',current_timestamp(),2021,1,'GET',
'/demo','/demo','Jakarta Commons-HttpClient/3.0-rc4',100,current_date());
insert into click_stream values(3,5901,'11.308.46.48',current_timestamp(),2022,33,'GET',
'/news','/partners/resell','Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',200,current_date());
insert into click_stream values(4,24538,'361.631.17.30',current_timestamp(),2021,30,'GET',
'/demo','-','Jakarta Commons-HttpClient/3.0-rc4',200,current_date());
insert into click_stream values(5,8116,'322.76.611.36',current_timestamp(),2022,30,'GET',
'/demo','-','Jakarta Commons-HttpClient/3.0-rc4',200,current_date());
insert into click_stream values(6,4675,'361.631.17.30',current_timestamp(),2021,0,'GET',
'/demo','-','Jakarta Commons-HttpClient/3.0-rc4',200,current_date());
insert into click_stream values(7,6575,'367.34.686.631',current_timestamp(),2022,0,'GET',
'/demo','-','Jakarta Commons-HttpClient/3.0-rc4',200,current_date());
insert into click_stream values(8,27382,'88.633.11.47',current_timestamp(),2021,30,'GET',
'/feeds/press','-','Apple-PubSub/65.12.1',200,current_date());
insert into click_stream values(9,9934,'11.308.46.48',current_timestamp(),2022,53,'GET',
'/partners','/product/product2','Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',200,current_date());
insert into click_stream values(10,1716,'683.615.622.618',current_timestamp(),2021,1,'PUT',
'/partners/resell','/partners/resell','null',200,current_date());
insert into click_stream values(11,55195,'682.62.387.672',current_timestamp(),2022,48,'GET',
'/download/download4.zip','/download/download4.zip','Mozilla/5.0 (Windows; U; Windows NT 5.1; es-ES; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3',100,current_date());
insert into click_stream values(12,69809,'43.68.686.668',current_timestamp(),2021,0,'GET',
'/feeds/press','-','Apple-PubSub/65.11',100,current_date());
insert into click_stream values(13,78800,'321.336.372.320',current_timestamp(),2022,0,'GET',
'/feeds/press','-','Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3',100,current_date());
insert into click_stream values(14,58994,'361.631.17.30',current_timestamp(),2021,0,'GET',
'/demo','/demo','Jakarta Commons-HttpClient/3.0-rc4',100,current_date());
insert into click_stream values(15,94543,'361.638.668.62',current_timestamp(),2022,30,'PUT',
'/ad/save','-','Mozilla/5.0 (Twiceler-0.9 http://www.cuil.com/twiceler/robot.html)',100,current_date());
insert into click_stream values(16,33695,'325.87.75.36',current_timestamp(),2021,30,'PUT',
'/demo','/demo','Jakarta Commons-HttpClient/3.0-rc4',100,current_date());

3. Create the below Spark Structure Streaming consumer application to pull the data from the topic and process the data.
{"id": 1, "custid": 1743, "ip": "68.688.326.58", "updts": "2023-10-05 00:48:14.0", "year": 2022, "visit_secs": 45, "http_verb": "GET", "short_url": "/products", "long_url": "/product/product4", "browser": "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Hewlett-Packard; .NET CLR 1.0.3705; .NET CLR 1.1.4322; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; MS-RTC LM 8; InfoPath.2)", "status_cd": 200, "upddt": "2023-10-05"}
{"id": 2, "custid": 75756, "ip": "361.631.17.30", "ts": "01/Nov/2021:10:53:01 -0500", "year": 2021, "visit_secs": 1, "http_verb": "GET", "short_url": "/demo", "long_url": "/demo", "browser": "Jakarta Commons-HttpClient/3.0-rc4", "status_cd": 100, "upddt": "2023-10-04"}

pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.0

from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *
schema = StructType([ 
        StructField("id", IntegerType(), True),
        StructField("custid" , IntegerType(), True),
        StructField("ip", StringType(), True),
        StructField("updts" , TimestampType(), True),
        StructField("year", IntegerType(), True),
        StructField("visit_secs" , IntegerType(), True),
        StructField("http_verb", StringType(), True),
        StructField("short_url" , StringType(), True),
        StructField("long_url" , StringType(), True),
        StructField("status_cd", IntegerType(), True),
        StructField("upddt" , DateType(), True)])
spark=SparkSession.builder.getOrCreate()
#number of spark DF partition = number of the topic partition
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094").option("subscribe", "weblogswd").option("kafka.group.id", "WE41-group").load()
#df.writeStream.format("console").start().awaitTermination()
#read the value from kafka topic -> convert to string -> convert to json applying the schema defined using from_json..
df1=df.select(from_json(col("value").cast("string"), schema).alias("json_value")).select("json_value.id","json_value.custid","json_value.visit_secs","json_value.http_verb","json_value.short_url","json_value.upddt","json_value.updts")
df1.createOrReplaceTempView("stream_view")
df2=spark.sql("select id,upper(short_url) as upper_short_url,visit_secs,upper(http_verb) as http_verb,upddt,updts from stream_view")
#df2.writeStream.format("console").start().awaitTermination()
query=df2.writeStream \
.outputMode("append") \
.queryName("appending_to_es") \
.format("org.elasticsearch.spark.sql") \
.option("checkpointLocation", "/tmp/ckptwe41") \
.option("es.resource", "idxwe41/webclicks") \
.option("es.nodes", "localhost") \
.option("es.mapping.id", "id") \
.start()
#df3=df2.write.mode("append").format("bigquery").option("tmpbucket","gs://...").option("table","abc")
query.awaitTermination()


#Usecase 2 - Structured Streaming basics

pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 --jars file:///home/hduser/install/elasticsearch-spark-30_2.12-7.12.0.jar 
#(or)
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,org.elasticsearch:elasticsearch-spark-30_2.12:7.12.0

1,Richard,100,89,90
2,Mary,100,89,80
3,Ann,90,89,60
4,Mary,60,49,90
5,Robert,80,55,90

#Open a Net Catalog Source in one terminal (Socket Buffer)
nc -lk 9999

#Socket Stream -> Console Output (Development/Testing usecases) - 1 ms latency
from pyspark.sql import *
from pyspark.sql.functions import *
df1=spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()
#1,Richard,100,89,90
df1 =  df1.select(split(col("value"),',').alias("stud"))
#Split of columns
#stud[1,Richard,100,89,90]
df1=df1.select(col("stud").getItem(0).alias("id"),col("stud").getItem(1).alias("name"),col("stud").getItem(2).alias("m1"),col("stud").getItem(3).alias("m2"),col("stud").getItem(4).alias("m3"))
#id,name,m1,m2,m3
#1,Richard,100,89,90
df1 = df1.withColumn("current_dt", current_date())
df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
df2.writeStream.format("console").start().awaitTermination()


#Socket Stream -> Console Output (Development/Testing/Debugging usecases - Processing Time example) 
from pyspark.sql import *
from pyspark.sql.functions import *
spark=SparkSession.builder.getOrCreate()
df1=spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()
df1 =  df1.select(split(col("value"), ",").alias("stud"))
df1=df1.select(col("stud").getItem(0).alias("id"),col("stud").getItem(1).alias("name"),col("stud").getItem(2).alias("m1"),col("stud").getItem(3).alias("m2"),col("stud").getItem(4).alias("m3"))
df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
df2.writeStream.format("console").trigger(processingTime='20 seconds').start()


#Usecase 3 - File Output Stream
#Socket Stream -> JSON Sink (Example of migrating the true realtime ingestion to near realtime ingestion into filesystems)
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
df1=spark.readStream.format("socket").option("host", "localhost").option("port",9999).load()
#df1.writeStream.format("console").start().awaitTermination()
df1 =  df1.select(split(col("value"), ",").alias("stud"))
df1=df1.select(col("stud").getItem(0).alias("id"),col("stud").getItem(1).alias("name"),col("stud").getItem(2).alias("m1"),col("stud").getItem(3).alias("m2"),col("stud").getItem(4).alias("m3"))
df1 = df1.withColumn("current_dt", current_date())
df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
df2.coalesce(1).writeStream.format("json").option("path", "/user/hduser/sparkstreamoutjsonwe41").option("checkpointLocation", "file:/tmp/sparkchkpointjson64").outputMode("append").trigger(processingTime='60 seconds').start().awaitTermination()
#df2.writeStream.format("console").start().awaitTermination()


#Usecase 3 - File Input Stream
#CSV Source -> Console
1|Richard|100|89|90
2|Mary|100|89|80
3|Ann|90|89|60

#Source system pushing LFS (.gz) -> Nifi (uncompress) -> HDFS/LFS -> Spark structure streaming CSV -> Transformation -> Console/NOSQL (Database)
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
studschema = StructType([StructField("studid",IntegerType(),True),StructField("studname",StringType(),True),StructField("m1",IntegerType(),True),StructField("m2",IntegerType(),True),StructField("m3",IntegerType(),True)])
df = spark.readStream \
.format("csv") \
.schema(studschema)\
.option("header", False)\
.option("maxFilesPerTrigger", 3)\
.option("sep","|")\
.option("path","file:/home/hduser/sparkstream")\
.load()
df1 = df.withColumn("current_dt", current_date())
df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
df2.writeStream.format("console").trigger(processingTime='30 seconds').start().awaitTermination()

#JSON -> tempview contains millisecond data -> SQL -> Console
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
bookschema = StructType([
StructField("title",StringType(),True),
StructField("author",StringType(),True),
StructField("year_written",IntegerType(),True),
StructField("edition",StringType(),True),
StructField("price",StringType(),True)]) 
df1 = spark.readStream.format("json").schema(bookschema).option("maxFilesPerTrigger", 2).option("path","file:/home/hduser/sparkstreamjson").load()
df1.createOrReplaceTempView("tblbooks")
df1 = spark.sql("select * from tblbooks where year_written > 2015")
df1.writeStream.format("console").start().awaitTermination()


#Usecase 4 - File Input & Output Stream (Realtime schema migration from csv to json)
#Source system pushing LFS (filename.csv.gz) -> Nifi (uncompress) -> HDFS -> Spark structure streaming CSV -> Transformation -> JSON (single file once in 30 seconds near realtime)
#CSV (1 millisecond data true realtime) -> JSON (30 seconds near realtime)
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.streaming import *
studschema = StructType([StructField("studid",IntegerType(),True),StructField("studname",StringType(),True),StructField("m1",IntegerType(),True),
StructField("m2",IntegerType(),True),StructField("m3",IntegerType(),True)])
df = spark.readStream \
.format("csv") \
.schema(studschema)\
.option("header", False)\
.option("maxFilesPerTrigger", 4)\
.option("sep","|")\
.option("path","file:/home/hduser/sparkstream")\
.load()
df1 = df.withColumn("current_dt", current_date())
df2 = df1.withColumn("TotalMarks", col("m1") + col("m2") + col("m3") )
df2.coalesce(1).writeStream.format("json").option("path", "/user/hduser/sparkstreamoutjsonwe41").option("checkpointLocation", "file:/tmp/sparkchkpointJSON12").outputMode("append")\
.trigger(processingTime='30 seconds').start()\
.awaitTermination()

#After this, external hive json handler table can be used.