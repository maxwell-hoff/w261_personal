# Databricks notebook source
# MAGIC %md # wk8 Demo - Advanced Spark - Pipelines and Optimizations with DataFrames
# MAGIC __`MIDS w261: Machine Learning at Scale | UC Berkeley School of Information | Spring 2019`__
# MAGIC 
# MAGIC So far we've been using Spark's low level APIs. In particular, we've been using the RDD (Resilient Distiributed Datasets) API to implement Machine Learning algorithms from scratch. This week we're going to take a look at how Spark is used in a production setting. We'll look at DataFrames, SQL, and UDFs (User Defined Functions).  As discussed previously, we still need to understand the internals of Spark and MapReduce in general to write efficient and scalable code.
# MAGIC 
# MAGIC In class today we'll get some practice working with larger data sets in Spark. We'll start with an introduction to efficiently storing data and approach a large dataset for analysis. After that we'll discuss a ranking problem which was covered in Chapter 6 of the High Performance Spark book and how we can apply that to our problem. We'll follow up with a discussion on things that could be done to make this more effiicent.
# MAGIC * ... __describe__ differences between data serialization formats.
# MAGIC * ... __choose__ a data serialization format based on use case.
# MAGIC * ... __change__ submission arguements for a `SparkSession`.
# MAGIC * ... __set__ custom configuration for a `SparkSession`.
# MAGIC * ... __describe__ and __create__ a data pipeline for analysis.
# MAGIC * ... __use__ a user defined function (UDF).
# MAGIC * ... __understand__ feature engineering and aggregations in Spark.
# MAGIC 
# MAGIC __`Additional Resources:`__ Writing performant code in Spark requires a lot of thought. Holden's High Performance Spark book covers this topic very well. In addition, Spark - The Definitive Guide, by Bill Chambers and Matei Zaharia, provides some recent developments.

# COMMAND ----------

# MAGIC %md ### Notebook Set-Up

# COMMAND ----------

# imports
import re
import os
import numpy as np
import pandas as pd

# COMMAND ----------

# MAGIC %md ### Load the data
# MAGIC Today we'll be using GSOD weather station data, avaliable from Google in BigQuery.
# MAGIC 
# MAGIC This dataset has been loaded into the MIDS S3 bucket and is available at:

# COMMAND ----------

DATA_PATH = 'dbfs:/mnt/mids-w261/DEMO8/gsod/'
dbutils.fs.ls(DATA_PATH)

# COMMAND ----------

# MAGIC %md ### Initialize Spark

# COMMAND ----------

# Here we show how to do a custom configuration
sc = spark.sparkContext

# COMMAND ----------

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# COMMAND ----------

sc.getConf().getAll()

# COMMAND ----------

# MAGIC %md # Exercise 1. Structured API - Datasets, DataFrames, and SQL Tables and Views
# MAGIC 
# MAGIC A Dataset is a distributed collection of data. Datasets provide the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL’s optimized execution engine. A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.). The Dataset API is available in Scala and Java. Python does not have the support for the Dataset API. But due to Python’s dynamic nature, many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally row.columnName). The case for R is similar.
# MAGIC 
# MAGIC A DataFrame is a Dataset organized into named columns. It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood. DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs. The DataFrame API is available in Scala, Java, Python, and R. In Scala and Java, a DataFrame is represented by a Dataset of Rows. In the Scala API, DataFrame is simply a type alias of Dataset[Row]. While, in Java API, users need to use Dataset<Row> to represent a DataFrame.
# MAGIC     
# MAGIC This makes the analysis of data similar to how we would do analysis with Python's Pandas or R's dataframes. Spark DataFrames are heavily inspired by Pandas and we're actually able to create Pandas user-defined functions (UDFs) to use with Spark which leverage the Apache Arrow project to vectorized computation instead of row-by-row operations. This can lead to significant performance boosts for large datasets. 
# MAGIC     
# MAGIC SQL Tables and Views are basically the same thing as DataFrames. We simply just execute SQL against them instead of DataFrame code  *(Defintive Guide, pg. 50)*. You can choose to express some of your data manipulations in SQL and others in DataFrames and they will compile to the same underlying code. *(Defintive Guide, pg. 179)*

# COMMAND ----------

# MAGIC %md  > __DISCUSSION QUESTIONS:__ 
# MAGIC  * _Why would we want to use RDDs in this class over DataFrames?_
# MAGIC  * _What is a UDF? Why do we need to create them?_
# MAGIC  * _What is vectorized computation and how does that differ from row-by-row function calls_
# MAGIC  * _How is a Dataset different than a DataFrame?_
# MAGIC  * _Are Datasets avaliable in the Python API?_

# COMMAND ----------

# MAGIC %md # Exercise 2. Data Serialization Formats. 
# MAGIC This week you read [Format Wars](http://www.svds.com/dataformats/) which covered the characteristics, structure, and differences between raw text, sequence, Avro, Parquet, and ORC data serializations. 
# MAGIC 
# MAGIC There were several points discussed: 
# MAGIC 
# MAGIC * Human Readable
# MAGIC * Row vs Column Oriented
# MAGIC * Read vs Write performance
# MAGIC * Appendable
# MAGIC * Splittable
# MAGIC * Metadata storage
# MAGIC 
# MAGIC *For additional information see Definitive Guide, Chapter 9: Data Sources, pg.153*

# COMMAND ----------

# MAGIC %md ## First let's understand our data

# COMMAND ----------

# MAGIC %md Here we see that we have several compressed CSV files as we expect based on our bq command specifying compressions. BigQuery was nice enough to split the files into 30 MB chunks so that our analysis will be partitioned nicely for ingestion.
# MAGIC 
# MAGIC Now let's try to ingest these CSV's without any special commands or unzipped.

# COMMAND ----------

data_csv = spark.read.option("header", "true").csv(DATA_PATH+"gsod-*.csv.gz")

# COMMAND ----------

data_csv.head()

# COMMAND ----------

data_csv.printSchema()

# COMMAND ----------

# MAGIC %%time
# MAGIC print((data_csv.count(), len(data_csv.columns)))

# COMMAND ----------

# MAGIC %md Wow that's nice we didn't even have to handle the decompression and it saves a ton on disk space! Next we're going to save this in a few different serializations so that we can see the effect on disk space.
# MAGIC 
# MAGIC Also notice that since we have 114 million observations and 31 columns we should see some huge performance boosts for compression in general and particularly columnar compression with parquet since it takes into account the data type to improve compression further. While row based compression will be less.
# MAGIC 
# MAGIC _Which Data Serialization do you think will do best?_

# COMMAND ----------

# MAGIC %md ### How do these look?

# COMMAND ----------

# MAGIC %md We have 4 data types below
# MAGIC 
# MAGIC - Compressed CSV
# MAGIC - Parquet
# MAGIC - Avro
# MAGIC - CSV
# MAGIC 
# MAGIC Of these, 3 are row oriented and 1 is column oriented. We have over 100M rows and 31 columns. Columnar compression should do fairly well in this scenerio. 

# COMMAND ----------

# RUN THIS CELL AS IS
# This code snippet reads the user directory name, and stores is in a python variable.
# Next, it creates a folder inside your home folder, which you will use for files which you save inside this notebook.
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
userhome = 'dbfs:/user/' + username
print(userhome)
DEMO8_path = userhome + "/DEMO8/" 
DEMO8_path_open = '/dbfs' + DEMO8_path.split(':')[-1] # for use with python open()
dbutils.fs.mkdirs(DEMO8_path)

# COMMAND ----------

data_csv.write.format("parquet").save(DEMO8_path+"data.parquet")

tot = 0
for item in dbutils.fs.ls(DEMO8_path+"data.parquet/"):
  tot = tot+item.size
tot
# ~1.7GB

# COMMAND ----------

data_csv.write.format("com.databricks.spark.avro").save(DEMO8_path+"data.avro")

tot = 0
for item in dbutils.fs.ls(DEMO8_path+"data.avro/"):
  tot = tot+item.size
tot
# ~6.8GB

# COMMAND ----------

data_csv.write.format("com.databricks.spark.csv").save(DEMO8_path+"data.csv")

tot = 0
for item in dbutils.fs.ls(DEMO8_path+"data.csv/"):
  tot = tot+item.size
tot
# ~23.5GB

# COMMAND ----------

# MAGIC %md ### How do these compare for simple computations?

# COMMAND ----------

# MAGIC %md First we need to read in the data again to ensure we're working with non-cached versions

# COMMAND ----------

data_parquet = spark.read.parquet(DEMO8_path+"data.parquet")

# COMMAND ----------

# MAGIC %md ### Count: 
# MAGIC Parquet keeps metadata about the data in order to compute some calculations extremely quickly such as row counts

# COMMAND ----------

# MAGIC %%time
# MAGIC data_parquet.count()

# COMMAND ----------

# MAGIC %%time
# MAGIC data_csv.count()

# COMMAND ----------

# MAGIC %md ### Average of a column: 
# MAGIC Parquet is column oriented so it can go through the sequence of data in one step instead of taking each row. This should have much higher performance

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %%time
# MAGIC data_parquet.agg(F.avg(data_parquet.max_temperature)).collect()

# COMMAND ----------

# MAGIC %%time
# MAGIC data_csv.agg(F.avg(data_csv.max_temperature)).collect()

# COMMAND ----------

# MAGIC %md  > __DISCUSSION QUESTIONS:__ For each key term from the reading, briefly explain what it means in the context of this demo code. Specifically:
# MAGIC  * _What is the compression ratio for the parquet to csv file?_
# MAGIC  * _Which serialization would query a column faster?_
# MAGIC  * _Which types of columns do you think has the best compression for parquet?_
# MAGIC  * _When should you use flat files vs other data formats?_
# MAGIC  * _If we want to do analysis with lots of aggregations what serialization should we use?_
# MAGIC  * _Is there any downside to Parquet?_
# MAGIC  * _If you had to partition data into days as new data comes in with aggregations happening at end of day how would you operationalize this?_

# COMMAND ----------

# MAGIC %md __INSTRUCTOR TALKING POINTS__
# MAGIC * What is the compression ratio for the parquet to csv file?
# MAGIC > We have 1.7G/21G = 0.081 or 8.1% of original size
# MAGIC 
# MAGIC * _Which serialization would query a column faster?_
# MAGIC > Parquet has a columnar format therefore a column of data has faster access and only needs to grab a subset of data
# MAGIC 
# MAGIC * _Which types of columns do you think has the best compression for parquet?_
# MAGIC > Columns with repeated content will have better compressions such as categorical columns will have very high compression ratios, especially if they're integers since parquet has enhanced compression for types with smaller storage requirements.
# MAGIC 
# MAGIC * _When should you use flat files vs other data formats?_
# MAGIC > If you need human readable data or you have small data sets
# MAGIC 
# MAGIC * _If we want to do analysis with lots of aggregations what serialization should we use?_
# MAGIC > Parquet
# MAGIC 
# MAGIC * _Is there any downside to Parquet?_
# MAGIC > Parquet is non-appendable which means that if we have new data coming in we can't grow the dataset with parquet. Parquet datasets are typically used for batch analysis after the data has reached a final state, such as on a date roll-over.
# MAGIC  
# MAGIC * _If you had to partition data into days as new data comes in with aggregations happening at end of day how would you operationalize this?_
# MAGIC > Data coming in for a day is streamed into an Avro file which handles appends seamlessly, then once the day has completed and a new partition for data is created a batch job can convert the avro file into a parquet file for the DS/Analyst team to query against.

# COMMAND ----------

# MAGIC %md # Exercise 3. Working with DataFrames and simple User-Defined Functions (UDFs)
# MAGIC 
# MAGIC In this example we're going to do some simple analysis of our data using built in spark functions. We'll look into UDFs and use a few instances of them to process our data

# COMMAND ----------

# Using built-in Spark functions are always more efficient
from pyspark.sql import types
import pyspark.sql.functions as F

timed = data_parquet.withColumn("time", F.concat(F.col("year"), F.lit("-"), F.col("month"), F.lit("-"), F.col("day")) \
                                .cast(types.TimestampType()))

# COMMAND ----------

# MAGIC %%time
# MAGIC timed.select('time').show(5)

# COMMAND ----------

# MAGIC %%time
# MAGIC timed.select('time').take(1)

# COMMAND ----------

# A simple UDF for converting year, month, day to timestamps
def create_date_from_parts(year, month, day):
    return f'{year}-{month}-{day}'

create_date_udf = F.udf(create_date_from_parts, types.StringType())
timed_udf = data_parquet.withColumn("date", create_date_udf('year', 'month', 'day').cast(types.TimestampType()))

# COMMAND ----------

# MAGIC %%time
# MAGIC timed_udf.take(1)

# COMMAND ----------

# MAGIC %md There's many things we could do from here but there are some important performance considerations when using UDFs. 
# MAGIC 
# MAGIC 
# MAGIC > User-defined functions and user-defined aggregate functions provide you with ways to extend the DataFrame and SQL APIs with your own custom code while keeping the Catalyst optimizer. The Dataset API (see “Datasets” on page 62) is another performant option for much of what you can do with UDFs and UDAFs. This is quite useful for performance, since otherwise you would need to convert the data to an RDD (and potentially back again) to perform arbitrary functions, which is quite expensive. (HP Spark pg 66) 
# MAGIC 
# MAGIC 
# MAGIC UDFs are typically much slower than built-in Spark functionality. The reason for this is becauase they have to serialize and deserialize the data for every row that the function is applied to. There have been recent improvements to UDF for some analytical results with Pandas UDFs that return scalars or groupby maps. Some more information about why UDFs are inefficent can be found here https://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/
# MAGIC 
# MAGIC Pandas UDFs solve the serialization issue by vectorizing the inputs and outputs, decreasing the serialziation from 3-100x; however, it isn't a golden bullet. See this blog for details http://garrens.com/blog/2018/03/04/using-new-pyspark-2-3-vectorized-pandas-udfs-lessons/
# MAGIC 
# MAGIC See also: http://sparklingpandas.com/

# COMMAND ----------

from IPython.display import Image
from IPython.core.display import HTML 
Image(url= "https://databricks.com/wp-content/uploads/2017/10/image1-4.png")

# COMMAND ----------

# MAGIC %md >__DISCUSSION QUESTION:__ 
# MAGIC * What is the task here? What did we really accomplish?
# MAGIC * What type does the UDF create_date_from_parts return?
# MAGIC * What information is being stored in the data frame? Is  there anything inefficient about this data structure? 
# MAGIC * What types of situations would lead to an inefficeint data structure in DataFrames? Could we be more efficient using an RDD in those situations?
# MAGIC * What questions would you ask of this table?

# COMMAND ----------

# MAGIC %md __INSTRUCTOR TALKING POINTS__
# MAGIC * What is the task here? What did we really accomplish?
# MAGIC > We read in data and created a datetime type so we have a single column we can organize data by instead of year, month, day.
# MAGIC 
# MAGIC * What type does the UDF create_date_from_parts return?
# MAGIC > String
# MAGIC 
# MAGIC * What information is being stored in the data frame? Is there anything inefficient about this data structure? 
# MAGIC > The data is mostly numerical. The data is stored effiicently since the DataFrame is mostly populated.
# MAGIC 
# MAGIC * What types of situations would lead to an inefficeint data structure in DataFrames? Could we be more efficient using an RDD in those situations?
# MAGIC > Sparse DataFrames have a more efficient data representation in RDDs. An example would be a multi-dimensional cube or pivot table (2d).
# MAGIC 
# MAGIC * What questions would you ask of this table?
# MAGIC > Do we have global warming? Let's look at it by looking at average tempertues by latitude have evolved over time.
# MAGIC 
# MAGIC UDFs, UDAFs, and Datasets all provide ways to intermix arbitrary code with Spark SQL.
# MAGIC 
# MAGIC We recommend you write your UDFs in Java or Scala-the small amount of time it atkes to write the function in Java or Scala will always yield significant speed ups. And you can still use the function from within python! (p.113 - Spark, The Definitive Guide)   
# MAGIC Scala UDF in python example:   
# MAGIC https://medium.com/wbaa/using-scala-udfs-in-pyspark-b70033dd69b9   
# MAGIC https://github.com/johnmuller87/spark-udf

# COMMAND ----------

# MAGIC %md #  Exercise 4. EDA
# MAGIC 
# MAGIC In this exercise we'll do some basic EDA/Sanity checks of our DataFrame, and start preparing it for our analysis in exercise 5.

# COMMAND ----------

timed.printSchema()

# COMMAND ----------

stations = spark.read.option("header", "true").csv(DATA_PATH+"stations.csv.gz")

# COMMAND ----------

stations.show(5)

# COMMAND ----------

# Let's filter for just the US since this is a US based dataset
from pyspark.sql import types
import pyspark.sql.functions as F
stations_us = stations.filter(F.col('Country')=='US')

'''
# Equivalently, we could write:
stations_us = stations.where('Country'=='US')
'''

# COMMAND ----------

stations_us.where(F.col('usaf')==722280).show(5)

# COMMAND ----------

# MAGIC %md There are two methods to perform this operation: you can use `where` or `filter` (as above) and they will both perform the same operation. (pg 74 - Spark, The Definitive Guide). Remember that the DataFrame API and Spark SQL compile to the same execution plan.
# MAGIC 
# MAGIC Take a look at the explain plan for pushdown predicates: (`PushedFilters: [IsNotNull(station_number)]`). (More info on pg 169, 325 - Spark, The Definitive Guide)  

# COMMAND ----------

# We need to bring that back to our timed dataframe
timed_stations = timed.join(F.broadcast(stations_us), stations_us.usaf==timed.station_number, 'inner')

# COMMAND ----------

timed_stations.explain()

# COMMAND ----------

# MAGIC %md Spark will automatically broadcast a small table, it's usually best to let Spark decide. See explain plan below. Notice that Spark broadcast the join for us even though we took that function out in our code. (p.151 Defintive Guide)

# COMMAND ----------

timed_stations_NB = timed.join(stations_us, stations_us.usaf==timed.station_number, 'inner')

# COMMAND ----------

timed_stations_NB.explain()

# COMMAND ----------

# Let's only keep what we care about so we minimize our pain
keep_columns = ['station_number', 'mean_temp', 'time', 'lat', 'lon']
temp = timed_stations.select(*keep_columns)

# COMMAND ----------

# Let's recast types
temp = temp.withColumn("mean_temp", temp["mean_temp"].cast(types.DoubleType()))
temp = temp.withColumn("lat", temp["lat"].cast(types.DoubleType()))
temp = temp.withColumn("lon", temp["lon"].cast(types.DoubleType()))
temp = temp.withColumn("station_number", temp["station_number"].cast(types.IntegerType()))

# COMMAND ----------

# MAGIC %%time
# MAGIC # How is our dataframe looking? We did filter a bunch of data
# MAGIC temp.describe().show()
# MAGIC # You could also use 'summary()' to extract individual numbers for future use.

# COMMAND ----------

def plot_hist(labels,values):
    df = pd.DataFrame({'lab':labels, 'val':values})
    df.plot.bar(x='lab', y='val', rot=0)

# COMMAND ----------

#temp.cache()

# COMMAND ----------

# We will use filter instead of rdd, and take advantage of predicate pushdown
import math
def makeHistogram(_min,_max,numBuckets,colName):
    _range = list(range(math.floor(_min), math.ceil(_max), round((abs(_min)+abs(_max))/numBuckets)))
    _counts = np.zeros(len(_range))
    for idx, val in enumerate(_range):
        if idx < len(_range)-1:
            _counts[idx] = temp.filter(F.col(colName) >= _range[idx]) \
                               .filter(F.col(colName) <= _range[idx+1]) \
                               .count()
    plot_hist(_range,_counts)

# COMMAND ----------

# MAGIC %%time
# MAGIC display(makeHistogram(-69.0,110.0,11,'mean_temp'))

# COMMAND ----------

# MAGIC %%time
# MAGIC display(makeHistogram(-179.63,179.583,11,'lon'))

# COMMAND ----------

# MAGIC %%time
# MAGIC display(makeHistogram(-60.483,80.13,11,'lat'))

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Using the RDD method (below) took almost an hour to run for a single plot. Even after caching the temp DF. Bummer.
# MAGIC 
# MAGIC ### Don't run these - it's for illustrative purposes only

# COMMAND ----------

# Let's look at some of the data in histograms
def plot_hist(hist_list):
    pd.DataFrame(
        list(zip(*hist_list)), 
        columns=['bin', 'frequency']
    ).set_index(
        'bin'
    ).plot(kind='bar')

# COMMAND ----------

# %%time
temp_hist = temp.select('mean_temp').rdd.flatMap(lambda x: x).histogram(11)
display(plot_hist(temp_hist))

# COMMAND ----------

# %%time
# temp_hist = temp.select('lon').rdd.flatMap(lambda x: x).histogram(11)

# # Loading the Computed Histogram into a Pandas Dataframe for plotting
# plot_hist(temp_hist)

# COMMAND ----------

# %%time
# temp_hist = temp.select('lat').rdd.flatMap(lambda x: x).histogram(11)

# # Loading the Computed Histogram into a Pandas Dataframe for plotting
# plot_hist(temp_hist)

# COMMAND ----------

# MAGIC %md # Exercise 5 - Analysis
# MAGIC 
# MAGIC In this section we're going to perform some aggregations to answer the question of "Is there global warming?" Don't take it too seriously, it's just an exercise! We're going to prepare our data so that we can draw an interactive graph displaying the change in avarage temperatures over time, as a function of latitude.

# COMMAND ----------

display(temp.take(10))

# COMMAND ----------

# let's use a struct to build a composite key
temp = temp.withColumn('time-lat', F.struct('time','lat'))
daily_average_at_latitude = temp.select('time-lat','mean_temp').groupBy("time-lat").agg(F.avg('mean_temp'))

# COMMAND ----------

# MAGIC %md If you’re used to RDDs you might be concerned by groupBy, but it is now a safe operation on thanks to the Spark SQL DataFrames optimizer, which automatically pipelines our reductions, avoiding giant shuffles and mega records. (HP Spark, pg. 43)

# COMMAND ----------

display(daily_average_at_latitude.take(10))

# COMMAND ----------

daily_average_at_latitude.printSchema()

# COMMAND ----------

# this function is here to make it easier to reason with the column names, flattens structs
def flatten_df(nested_df):
    flat_cols = [c[0] for c in nested_df.dtypes if c[1][:6] != 'struct']
    nested_cols = [c[0] for c in nested_df.dtypes if c[1][:6] == 'struct']

    flat_df = nested_df.select(flat_cols +
                               [F.col(nc+'.'+c).alias(nc+'_'+c)
                                for nc in nested_cols
                                for c in nested_df.select(nc+'.*').columns])
    return flat_df

# COMMAND ----------

daily_average_at_latitude = flatten_df(daily_average_at_latitude)

# COMMAND ----------

daily_average_at_latitude.printSchema()

# COMMAND ----------

# Now let's get the average on each latitude
daily_average_at_latitude = daily_average_at_latitude.withColumn('time-rounded-lat', F.struct('time-lat_time',F.round(daily_average_at_latitude['time-lat_lat'],0)))
average_by_lat = daily_average_at_latitude.groupby('time-rounded-lat').agg(F.avg('avg(mean_temp)'))

# COMMAND ----------

display(average_by_lat.take(10))

# COMMAND ----------

# we used the struct in order to do a simple groupby that we can flatten again to get information
average_by_lat.printSchema()

# COMMAND ----------

# the names weren't very descriptive, let's rework that
average_by_lat = flatten_df(average_by_lat)
average_by_lat = average_by_lat.withColumnRenamed('time-rounded-lat_col2', 'rounded-lat')
average_by_lat = average_by_lat.withColumnRenamed('time-rounded-lat_time-lat_time', 'time')
average_by_lat = average_by_lat.withColumnRenamed('avg(avg(mean_temp))', 'temp')
average_by_lat.printSchema()

# COMMAND ----------

# oh man that's a lot of stuff and since we can't cache the data this is taking forever to run.
average_by_lat.explain()

# COMMAND ----------

# Let's output to file and then read from that file to reduce our load.
average_by_lat.write.format("parquet").save(DEMO8_path+"average_by_lat.parquet")
average_by_lat_read = spark.read.parquet(DEMO8_path+"average_by_lat.parquet")

# COMMAND ----------

average_by_lat_read.explain()

# COMMAND ----------

average_by_lat_read.show(10)

# COMMAND ----------

# Small enough to fit in pandas for our final analysis. Let's do that
average_by_lat_read.count()

# COMMAND ----------

df = average_by_lat_read.toPandas()

# COMMAND ----------

df.head()

# COMMAND ----------

df = df.set_index(['rounded-lat','time'])

# COMMAND ----------

df = df.sort_index()

# COMMAND ----------

df.head()

# COMMAND ----------

lat_list = df.index.levels[0]
print(lat_list)

# COMMAND ----------

# MAGIC %md > __DISCUSSION QUESTIONS:__
# MAGIC * Why did we create a struct for our groupBy?
# MAGIC * Why did we push our transformations to a file and load them again? - compare and contrast cache(), save to disk (checkpoint), saveManagedTable
# MAGIC * Where could we have done this before to save computation time?
# MAGIC * Why did we do a rolling average of temperature?
# MAGIC * Isn't pandas a lot easier to use?

# COMMAND ----------

# MAGIC %md __INSTRUCTOR TALKING POINTS__
# MAGIC * Why did we create a struct for our groupBy?
# MAGIC > This allowed us to create a composite key that we can access the original keys in a seamless fashion
# MAGIC 
# MAGIC * Why did we push our transformations to a file and load them again?
# MAGIC > This offload allowed us to have non-repeated computation once we went through the transformation steps. 
# MAGIC 
# MAGIC * Where could we have done this before to save computation time?
# MAGIC > We could have done a similar file offload prior to generating our histograms that repeated the computation 3x.
# MAGIC 
# MAGIC * Why did we do a rolling average of the temprature?
# MAGIC > We only care about the long term change in temperature and the day to day variations are dependent on various weather effects such as rain or clouds.
# MAGIC 
# MAGIC * Isn't pandas a lot easier to use?
# MAGIC > Yes, but it also has significant limitations in comparison to the raw data we're able to process with Spark. 

# COMMAND ----------

# lets save this df for later plotting, just in case. We're still in the cloud here.
df.to_csv(DEMO8_path_open+"pandas.csv")

# COMMAND ----------

# MAGIC %md # Interactive chart
# MAGIC Databricks does not have the ipywidgets module. If you want to run this section, you will need to save the pandas.csv file to your local machine, and open this notebook in jupyter notebook (NOT jupyter lab).

# COMMAND ----------

import pandas as pd

# COMMAND ----------

df = pd.read_csv("pandas.csv")

# COMMAND ----------

df.head()

# COMMAND ----------

df = df.set_index(['rounded-lat','time'])

# COMMAND ----------

df = df.sort_index()

# COMMAND ----------

lat_list = df.index.levels[0]
print(lat_list)

# COMMAND ----------

# Is temperature increasing? Data isn't very clean and we didn't perform any sensor corrections.
%matplotlib notebook
#%matplotlib inline
from ipywidgets import *
import numpy as np
import matplotlib.pyplot as plt

n_roll = 365
def f(x):
    df.loc[x].rolling(n_roll).mean().plot()

interact(f, x=lat_list);