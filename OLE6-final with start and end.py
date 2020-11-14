# Databricks notebook source

from pyspark.sql import SparkSession

# COMMAND ----------

from pyspark.sql.types import DateType

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

spark = SparkSession.builder.appName("INRIX3").getOrCreate()

# COMMAND ----------

#dfsegment = spark.read.csv("/FileStore/tables/INRIX_XDSegments_18_1.csv", inferSchema=True, header=True)

# COMMAND ----------

dfsegment = spark.read.csv("/reactor/davami/INRIX_XDSegments_18_1.csv", inferSchema=True, header=True)

# COMMAND ----------

dfinrix = spark.read.csv("/reactor/INRIX_old/2018/11/11-4-2018.csv", inferSchema=True, header=True)

# COMMAND ----------

#dfinrix = spark.read.csv("/FileStore/tables/10_1_2018small-3b9d9.csv", inferSchema=True, header=True)

# COMMAND ----------

#dfinrix = spark.read.csv("/FileStore/tables/11_4_2018small-1.csv", inferSchema=True, header=True)

# COMMAND ----------

dfinrix.show()

# COMMAND ----------

dfdateadd = dfinrix.withColumn("Date",dfinrix["CentralTime"].cast(DateType()))

# COMMAND ----------

dfdate = dfdateadd.drop("SegmentClosed", "CentralTime", "Time")

# COMMAND ----------

dfspeed = dfdate.filter("Speed>0")

# COMMAND ----------

dfmile = dfspeed.join(dfsegment,dfspeed.Code == dfsegment.XDSegID).select(dfspeed["*"],dfsegment["Miles"])

# COMMAND ----------

dfdel = dfmile.withColumn("RefDel",(dfmile["Travel"]/60-dfmile["Miles"]/dfmile["Reference"])).withColumn("AvgDel",(dfmile["Travel"]/60-dfmile["Miles"]/dfmile["Average"]))

# COMMAND ----------

reffunc = (F.when(F.col("RefDel") < 0, 0).otherwise(F.col("RefDel")))

# COMMAND ----------

avgfunc = (F.when(F.col("AvgDel") < 0, 0).otherwise(F.col("AvgDel")))

# COMMAND ----------

dfdela = dfdel.withColumn("RefDelay",reffunc).withColumn("AvgDelay",avgfunc)

# COMMAND ----------

dfdelay = dfdela.drop("RefDel","AvgDel")

# COMMAND ----------

dftotalcount = dfdelay.groupby("Code", "Date").count().withColumnRenamed("count","TotalCount")

# COMMAND ----------

dffilt10 = dfdelay.filter(dfdelay.Score == 10)

# COMMAND ----------

dffilter10 = dffilt10.groupby("Code", "Date").count().withColumnRenamed("count","Count10")

# COMMAND ----------

dffilt20 = dfdelay.filter(dfdelay.Score == 20)

# COMMAND ----------

dffilter20 = dffilt20.groupby("Code","Date").agg({"Code":"Count","Speed":"avg","Average":"avg","Reference":"avg","Travel":"avg","RefDelay":"sum","AvgDelay":"sum"}).withColumnRenamed("avg(Speed)","Speed20").withColumnRenamed("avg(Average)","AvgSpeed20").withColumnRenamed("avg(Reference)","RefSpeed20").withColumnRenamed("avg(Travel)","Traveltime20").withColumnRenamed("sum(RefDelay)","RefDelay20").withColumnRenamed("sum(AvgDelay)","AvgDelay20").withColumnRenamed("count(Code)","Count20")

# COMMAND ----------

dffilt30 = dfdelay.filter(dfdelay.Score == 30)

# COMMAND ----------

dffilter30 = dffilt30.groupby("Code","Date").agg({"Code":"Count","C-Value":"avg","Speed":"avg","Average":"avg","Reference":"avg","Travel":"avg","RefDelay":"sum","AvgDelay":"sum"}).withColumnRenamed("avg(Speed)","Speed30").withColumnRenamed("avg(Average)","AvgSpeed30").withColumnRenamed("avg(Reference)","RefSpeed30").withColumnRenamed("avg(Travel)","Traveltime30").withColumnRenamed("sum(RefDelay)","RefDelay30").withColumnRenamed("sum(AvgDelay)","AvgDelay30").withColumnRenamed("count(Code)","Count30").withColumnRenamed("avg(C-Value)","Cvalue")

# COMMAND ----------

dfcodeo = dfsegment.select("XDSegID").withColumnRenamed("XDSegID","CodeO")

# COMMAND ----------

dfcode = dfcodeo.withColumn("Code",dfcodeo.CodeO.astype("Int")).drop("CodeO")

# COMMAND ----------

dftotalcountmix = dfcode.join(dftotalcount, on=["Code"], how="left")

# COMMAND ----------

dfjoin10 = dftotalcountmix.join(dffilter10, on=["Code","Date"], how="left")

# COMMAND ----------

dfjoin20 = dfjoin10.join(dffilter20, on=["Code","Date"], how="left")

# COMMAND ----------

dfjoin30 = dfjoin20.join(dffilter30, on=["Code","Date"], how="left")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import substring, hour, minute

# COMMAND ----------

dfinrixm = dfinrix.filter("Speed<0.6 * Reference")

# COMMAND ----------

dftimestamp = dfinrixm.withColumn('NT', substring('CentralTime', 1, 19).astype("Timestamp")).drop("C-Value","SegmentClosed","Score","Speed","Average","Reference","Travel","Time")

# COMMAND ----------

dfmsm = dftimestamp.withColumn("msm",hour(dftimestamp.NT)*60 + minute(dftimestamp.NT)).drop("CentralTime")

# COMMAND ----------



# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, ArrayType, StructType

# COMMAND ----------

dfb = dfmsm.sort("Code","msm")

# COMMAND ----------

dfc = dfb.groupBy("Code").agg({"msm":"collect_list"}).withColumnRenamed("collect_list(msm)","lowSpeed")

# COMMAND ----------

def calculate(mylist):
    #mylist=[4, 35, 36, 37, 38, 39, 47,64,78, 79, 80, 81,82,83, 140 ]  
    tduration = 0
    duration = 0
    avg = 0
    bottleneck = 0
    y = 0
    x = 0
    z = 0
    v = 0
    blist = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    clist = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    while x<len(mylist)-4 and y<len(mylist)-1 :

        if mylist[x+4] == mylist[x]+4:
            y = x + 4
            blist[z]=mylist[x]
            z = z + 1
            print("MY LIST X = ",mylist[x])
            print("X = ", x)
            print ("Y = ", y)
            print("blist = ", blist)
            while True:
                if y==len(mylist)-1 or mylist[y+1] > mylist[y]+10:
                    clist[v]=mylist[y]
                    v = v + 1
                    print("clist = ", clist)
                    bottleneck = bottleneck + 1
                    duration = mylist[y] - mylist[x] + 1
                    tduration = tduration + duration 
                    avg = 1.0*tduration/bottleneck
                    x = y + 1
                    print("MY LIST Y = " , mylist[y])
                    print("Duration = " , duration)
                    print("average = " , avg)
                    break
                else:
                    y = y + 1
        else:
            x = x + 1
    return bottleneck, avg, blist[0],blist[1],clist[0], clist[1]

# COMMAND ----------

func = F.udf(lambda x: calculate(x), T.StructType(
        [T.StructField("val1", T.IntegerType(), True),
         T.StructField("val2", T.FloatType(), True),
         T.StructField("val3", T.IntegerType(), True),
         T.StructField("val4", T.IntegerType(), True),
         T.StructField("val5", T.IntegerType(), True),
         T.StructField("val6", T.IntegerType(), True)]))

# COMMAND ----------

dfd = dfc.withColumn('vals', func('lowSpeed'))

# COMMAND ----------

dfd.show()

# COMMAND ----------

dfe = dfd.withColumn("BottleneckCount",col('vals.val1')).withColumn("AverageDuration",col('vals.val2')).withColumn("Bottleneck1 start",col('vals.val3')).withColumn("Bottleneck1 end",col('vals.val5')).withColumn("Bottleneck2 start",col('vals.val4')).withColumn("Bottleneck2 end",col('vals.val6')).drop("lowSpeed","vals")

# COMMAND ----------

dffinal1 = dfjoin30.join(dfe, on=["Code"], how="left")

# COMMAND ----------

dffinal2 = dffinal1.filter("TotalCount>5")

# COMMAND ----------

#dffinal2.coalesce(1).write.csv("/FileStore/output1/", header =True)

# COMMAND ----------

#dffinal2.show(100)

# COMMAND ----------

#display(dffinal2)

# COMMAND ----------

#dffinal2.registerTempTable("dffinal2")
#sqlContext.sql("""SELECT AverageDuration FROM dffinal2""").show


# COMMAND ----------

#dffinal2.printSchema()

# COMMAND ----------
#dffinal2.repartition(1).write.format("csv").option("header", "true").save("/reactor/davami/mydata.csv")
dffinal2.coalesce(1).write.csv("/reactor/davami/olefeed5/", header=True)

