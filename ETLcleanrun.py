from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
# define spark configuration object
spark = SparkSession.builder\
   .appName("Very Important SQL End to End App") \
    .config("spark.jars","file:///home/hduser/install/mysql-connector-java.jar")\
    .enableHiveSupport()\
   .getOrCreate()
#.config("spark.jars", "/usr/local/hive/lib/mysql-connector-java.jar")\
# .config("spark.jars", jdbc_lib)\
#spark.conf.set("hive.metastore.uris","thrift://127.0.0.1:9083")
#.config("hive.metastore.uris", "thrift://127.0.0.1:9083") \
# to connect with Remote metastore, but we can't do it in Organization when we develop the pyspark app using Pycharm running in Windows

# Set the logger level to error
spark.sparkContext.setLogLevel("ERROR")

custstructtype1 = StructType([StructField("id", IntegerType(), False),
                              StructField("custfname", StringType(), False),
                              StructField("custlname", StringType(), True),
                              StructField("custage", ShortType(), True),
                              StructField("custprofession", StringType(), True)])

custdf_clean=spark.read.csv("file:///home/hduser/hive/data/custsmodified",mode='dropmalformed',schema=custstructtype1)
custdf_clean.printSchema()#clean data frame without datatype mismatch and without number of columns mismatches
custdf_clean.show(20,False)

dedup_custdf_clean=custdf_clean.distinct()
dedup_custdf_clean.where("id =4000001").show()
#or
dedup_custdf_clean=custdf_clean.dropDuplicates()
dedup_custdf_clean.where("id =4000001").show()

dedupcol_custdf_clean=custdf_clean.dropDuplicates(subset=["id"])#dropduplicates will retain only the first duplicate record
dedupcol_custdf_clean.where("id =4000003").show()

dedupcol_custdf_clean=custdf_clean.sort("custage",ascending=False).dropDuplicates(subset=["id"])
dedupcol_custdf_clean.where("id =4000003").show()

#Cleansed Dataframe for further scrubbing
dedupcol_custdf_cleansed_id_notnull=dedupcol_custdf_clean.na.drop(how="any",subset=["id"])
dedupcol_custdf_cleansed_id_notnull.where("id is null").show()

print("Scrubbing (convert of raw to tidy na.fill or na.replace)")

dedupcol_custdf_cleansed_profession_na=dedupcol_custdf_cleansed_id_notnull.na.fill("not given",subset=["custlname"]).na.fill("profession not given",subset=["custprofession"])

dict1={"Reporter":"Media","Musician":"Instrumentist"}
dedupcol_custdf_cleansed_profession_scrubbed_replace=dedupcol_custdf_cleansed_profession_na.withColumn("srcsystem",lit("Retail")).na.replace(dict1,subset=["custprofession"])

reord_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custage","custlname","custfname","custprofession")#reordering
reord_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custlname","custfname","custprofession")#removing
reord_added_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id","custage",lit("custdata").alias("typeofdata"),"custlname","custfname","custprofession")#adding
rename_replace_reord_added_removed_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.select("id",col("custage").alias("age"),col("custlname").alias("custfname"),"custprofession")
rename_replace_reord_added_removed_df.show()

#withColumn - (preferred for adding columns in the last)
add_col_df=dedupcol_custdf_cleansed_profession_scrubbed_replace.withColumn("sourcesystem",lit("retail"))

munged_df=dedupcol_custdf_cleansed_profession_scrubbed_replace

#select,drop,withColumn,withColumnRenamed
#Adding of columns (withColumn/select) - for enriching the data
enrich_addcols_df6=munged_df.withColumn("loaddt",current_date()).withColumn("loadts",current_timestamp())#both are feasible

enrich_ren_df7=enrich_addcols_df6.withColumnRenamed("srcsystem","src")#preferrable way (delete srcsystem and create new column src without changing the order)
#Concat to combine/merge/melting the columns
enrich_combine_df8=enrich_ren_df7.select("id","custfname","custprofession",concat("custfname",lit(" is a "),"custprofession").alias("nameprof"),"custage",lit("Retail").alias("sourcesystem"),"loaddt","loadts")

#Splitting of Columns to derive custfname
enrich_combine_split_df9=enrich_combine_df8.withColumn("custfname",split("nameprof",' ')[0])
enrich_combine_split_df9.show(2)
enrich_combine_split_cast_df10=enrich_combine_split_df9.withColumn("curdtstr",col("loaddt").cast("string")).withColumn("year",year(col("loaddt"))).withColumn("yearstr",substring("curdtstr",1,4))
enrich_combine_split_cast_df10.show()

enrich_combine_split_cast_reformat_df10=enrich_combine_split_df9.withColumn("sourcesystem",upper("sourcesystem"))\
    .withColumn("fmtdtstr",concat(substring("loaddt",6,2),lit("/"),substring("loaddt",9,2)))\
    .withColumn("fmtloaddt",date_format("loadts",'yyyy-MM-dd')).withColumn("dtfmt",date_format("loaddt",'yyyy/MM/dd hh:mm:ss')).withColumn("tsfmt",to_timestamp(date_format("loaddt",'yyyy-MM-dd hh:mm:ss')))

munged_enriched_df=enrich_combine_split_cast_reformat_df10

convertToUpperPyLamFunc=lambda prof:prof.upper()

from pyspark.sql.functions import udf

convertToUpperDFUDFFunc=udf(convertToUpperPyLamFunc)

spark.udf.register("convertToUpperPyLamFuncSQL",convertToUpperPyLamFunc)

customized_munged_enriched_df=munged_enriched_df.withColumn("custprofession",convertToUpperDFUDFFunc("custprofession"))
customized_munged_enriched_df.show(2)

predefined_munged_enriched_df=munged_enriched_df.withColumn("custprofession",upper("custprofession"))
predefined_munged_enriched_df.show(2)

from pyspark.sql.functions import udf
def ageCat(age):
    if age < 13:
        return 'children'
    elif age>=13 and age<=18:
        return 'teen'
    else:
        return 'adult'

dslAgeGroup=udf(ageCat)
customize_df1=enrich_combine_split_cast_reformat_df10.withColumn("agegroup",dslAgeGroup("custage"))
customize_df1.show(5,False)

custom_agegrp_munged_enriched_df=munged_enriched_df.select("*",expr("case when custage<13 then 'Children' when custage>=13 and custage<=18 then 'Teen' else 'Adults' end as agegroup"))

#Step6: Display the dataframe and Filter the records based on age_group
custom_agegrp_munged_enriched_df.show(5,False)
custom_agegrp_munged_enriched_df.filter("agegroup = 'Adults'").show(5,False)

pre_wrangled_customized_munged_enriched_df=custom_agegrp_munged_enriched_df.select("id","custprofession","custage",col("sourcesystem").alias("src"),"loaddt")\
    .groupBy("custprofession")\
    .agg(avg("custage").alias("avgage"))\
    .where("avgage>49")\
    .orderBy("custprofession")

#Filter rows and columns
#select * from where
filtered_adult_row_df_for_consumption1=custom_agegrp_munged_enriched_df.where("agegroup='Adults'")#Will be used in the Data persistant last stage
#select few-columns from where
filtered_nochildren_rowcol_df_for_further_wrangling1=custom_agegrp_munged_enriched_df.filter("agegroup<>'Children'").select("id","custage","loaddt","custfname","agegroup")#Will be used in the next stage for further wrangling

randomsample1_for_consumption3=custom_agegrp_munged_enriched_df.sample(.2,10)#Consumer (Datascientists needed for giving training to the models)

txnsstructtype2=StructType([StructField("txnid",IntegerType(),False),StructField("dt",StringType(),False)])
txns = spark.read.option("inferschema", True)\
.option("header", False).option("delimiter", ",")\
.csv("file:///home/hduser/hive/data/txns")\
.toDF("txnid", "dt", "custid", "amt", "category", "product", "city", "state", "transtype").na.drop("all")

txns_dtfmt=txns.withColumn("dt",to_date("dt",'MM-dd-yyyy'))
txns_dtfmt.show(2)#dt is converted to yyyy-MM-dd
txns_dtfmt.printSchema()#dt is converted from string to date

enriched_dt_txns=txns_dtfmt.withColumn("yr",year("dt")).withColumn("mth",month("dt")).withColumn("lastday",last_day("dt")).withColumn("nextsunday",next_day("dt",'Sunday')).withColumn("dayofwk",dayofweek("dt")).withColumn("threedaysadd",date_add("dt",3)).withColumn("datediff",datediff(current_date(),"dt")).withColumn("daytrunc",trunc("dt",'mon'))
enriched_dt_txns.show(2,False)

cust_df=filtered_nochildren_rowcol_df_for_further_wrangling1

cust_df.join(enriched_dt_txns,on=[col("id")==col("custid")]).show(2)


