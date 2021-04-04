from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder\
    .config("spark.sql.shuffle.partitions", "2")\
    .appName("demo03")\
    .master("local[2]")\
    .getOrCreate()


parDF=spark.read\
    .option("inferSchema", "true")\
    .parquet("/home/sunbeam/Downloads/consumerInternet.parquet")
parDF.show()
parDF.printSchema()

df = spark.read.csv('/home/sunbeam/Downloads/startup.csv', header = True)
df.repartition(1).write.mode('overwrite').parquet('/home/sunbeam/Downloads/startup')

df = spark.read.parquet('/home/sunbeam/Downloads/startup')
df.show()
df.printSchema()

df_merged=parDF.union(df)
df_merged.show()

df_merged.createOrReplaceTempView("ParquetTable")
parsql1=spark.sql("select Startup_Name,City from ParquetTable where City=='Pune'")
parsql1.show()
parsql1.coalesce(1).write.format('csv').save("/tmp/output1/1.csv", header='true')

parsql2=spark.sql("select count(Startup_Name) from ParquetTable where City=='Pune' and InvestmentnType=='Seed/ Angel Funding'")
parsql2.show()
parsql2.coalesce(1).write.format('csv').save("/tmp/output1/2.csv", header='true')




parsql3=spark.sql("select SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT)) totalpunefund from ParquetTable where City=='Pune'")
parsql3.show()
parsql3.coalesce(1).write.format('csv').save("/tmp/output1/3.csv", header='true')

parsql4=spark.sql("select Industry_Vertical, count(Startup_Name) startup_count from ParquetTable group by Industry_Vertical order by startup_count desc limit 5")
parsql4.show()
parsql4.coalesce(1).write.format('csv').save("/tmp/output1/4.csv", header='true')

parsql5=spark.sql("select Investors_Name, (CAST(regexp_replace(SUBSTRING(Date,6,10), '[^0-9]*', '') AS BIGINT)) date, SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT)) amount from ParquetTable where Investors_Name!='N/A' group by Investors_Name, date ORDER BY amount desc")
parsql5.show()
parsql5.coalesce(1).write.format('csv').save("/tmp/output1/5.csv", header='true')

#parsql6=spark.sql("select Startup_Name from ParquetTable where City=(select City, SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT)) total from ParquetTable where City!='nan' group by City order by total desc)")
#parsql6.show()
#parsql6.coalesce(1).write.format('csv').save("/tmp/output1/6.csv", header='true')





parsql6=spark.sql("select City, SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT)) total from ParquetTable  where City!='nan' group by City order by total desc")
parsql6.show()

#parsql7=spark.sql("select MAX(mycount) from (select COUNT(Startup_Name) mycount from  ParquetTable where SubVertical!='nan' group by SubVerticaL)")
#parsql7.show()
#parsql7.coalesce(1).write.format('csv').save("/tmp/output1/7.csv", header='true')

parsql7=spark.sql("select SubVertical,COUNT(Startup_Name) from ParquetTable  group by SubVertical HAVING COUNT(Startup_Name)=(select MAX(mycount) from (select COUNT(Startup_Name) mycount from  ParquetTable where SubVertical!='nan' group by SubVertical))")
parsql7.show()
parsql7.coalesce(1).write.format('csv').save("/tmp/output1/8.csv", header='true')


#parsql9=spark.sql("select SubVertical,COUNT(Startup_Name) from ParquetTable  group by SubVertical HAVING COUNT(Startup_Name)=(select MAX(mycount) from (select COUNT(Startup_Name) mycount from  ParquetTable where SubVertical!='nan' group by SubVertical))")
#parsql9.show()
#parsql9.coalesce(1).write.format('csv').save("/tmp/output1/9.csv", header='true')

parsql10=spark.sql("select SubVertical, SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT)) totalfund from ParquetTable group by SubVertical HAVING SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT))=(select MAX(totalamount) from (select (SUM(CAST(regexp_replace(Amount_in_USD, '[^0-9]*', '') AS BIGINT))) totalamount from ParquetTable where SubVertical!='nan' group by SubVertical))")
parsql10.show()
parsql10.coalesce(1).write.format('csv').save("/tmp/output1/10.csv", header='true')



