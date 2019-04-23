# Databricks notebook source
# MAGIC %md
# MAGIC Welcome to Spark Introduction notebook. Remember to check out the [documentation](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html).

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
import pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC Though it is possible to create Spark DataFrame directly, it is somewhat cumbursome. Quite often I create Pandas object first and convert it to Spark later.

# COMMAND ----------

data = {'TimeStamp': [1, 2, 3, 4, 8, 9, 1, 2, 3],
       'StationId': [1, 1, 1, 1, 1, 1, 2, 2, 2],
       'value': [1, 2, 3, 4, 5, 6, 4, 5, 6]}
pandas_df = pd.DataFrame(data)
df = spark.createDataFrame(pandas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Simple visualizations are done by using `show` method.

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC A bit more advanced use display ([docs](https://docs.databricks.com/user-guide/visualizations/index.html)) function. Note that display will only use the first 1000 rows of your dataframe!

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC # Basic DataFrame operations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Here is how we can select columns

# COMMAND ----------

display(df.select('StationId'))

# COMMAND ----------

displayHTML('<h1 style="color: #009345;">Can we select multiple columns? Try it out in the cell below!</h1>')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Basic row filtering

# COMMAND ----------

# MAGIC %md
# MAGIC Column expression is given in a string. This is basically SQL syntax.

# COMMAND ----------

df.filter('StationId == 2').show()

# COMMAND ----------

# MAGIC %md
# MAGIC One can use a column object. See that we use `F`, which stands for `pyspark.sql.functions`. We imported it in the beginning of the notebook.

# COMMAND ----------

df.filter(F.col('StationId') == 2).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Yet another way of doing it by using function `lit` ([docs](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.lit)). The function creates a Column of literal value.

# COMMAND ----------

df.filter(F.col('StationId') == F.lit(2)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC One can combine filter with selection. The order of select/filter does not matter.

# COMMAND ----------

df.select(df['value']).filter('StationId == 2').show()

# COMMAND ----------

displayHTML('<h1 style="color: #009345;">Do you think the order of select/filter matters?</h1>')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Column aliases
# MAGIC 
# MAGIC Once you start to use more advanced functions/transformations you will need yet another function `alias`, which returns a column aliased with a new name or names.
# MAGIC 
# MAGIC Consider these two examples:

# COMMAND ----------

df.select('TimeStamp', 'StationId', 'value', F.pow(F.col('value'), F.lit(2))).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Note also the usage of `*`. SQL...

# COMMAND ----------

df.select('*', F.pow(F.col('value'), F.lit(2)).alias('powered')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lazy evaluation
# MAGIC 
# MAGIC Let's see the effect of lazy evaluation.

# COMMAND ----------

# Here we are going to create 1e8 random numbers. We will sort them afterwards.
df1 = spark.range(1e8).toDF('id')
df1 = df1.select("id", F.randn(seed=27).alias("normal"))

# COMMAND ----------

# Sort by 'normal' column
df2 = df1.sort('normal')

# COMMAND ----------

# MAGIC %md
# MAGIC Execution time is `0.03` seconds! Not bad for sorting 1e8 numbers. And the reason for this is that there have been no sorting operation so far. Spark has just record that `df2` is constructed by sorting `df1`. Try to do an action on `df2` (like count number of rows, display it, write it to disc) and then Spark will perform the sorting.

# COMMAND ----------

displayHTML('<h1 style="color: #009345;">Well go on now! Check out execution time when Spark actually does the job.</h1>')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Caching
# MAGIC 
# MAGIC If you now will try to do the same or some other operation on `df2`, this will result in the recalculation of `df2`. This might be very inefficient if `df2` takes time to compute. One receipe to avoid this is to use caching.

# COMMAND ----------

df2.cache()
df2.count()

# COMMAND ----------

df2.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Physical plans
# MAGIC 
# MAGIC Physical plans can be viewed by calling `explain` on any dataframe. They are read from top to bottom. Top is the end result. Bottom is the data source.

# COMMAND ----------

# MAGIC %md
# MAGIC These two below are narrow transformations.

# COMMAND ----------

df.explain()

# COMMAND ----------

df1.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC `df2` is constructed via `sort`. It is a wide transformation and thus there is `Exchange` step in the physical plan.

# COMMAND ----------

df2.explain()

# COMMAND ----------

df.groupBy('StationId').max('value').explain()

# COMMAND ----------

# MAGIC %md
# MAGIC # Joins and shuffles
# MAGIC 
# MAGIC Let's look at how joins are performed

# COMMAND ----------

display(df)

# COMMAND ----------

# Create a dataframe with 'label'. Note that we do not have all the TimeStamps from df!
data = {'TimeStamp': [1, 3, 8, 9, 1, 2],
       'StationId': [1, 1, 1, 1, 2, 2],
       'label': [0, 0, 1, 0, 0, 1]}
pandas_df = pd.DataFrame(data)
labels_df = spark.createDataFrame(pandas_df)
display(labels_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Default join type is INNER meaning the result will contain rows that are present in both data frames.
# MAGIC 
# MAGIC **Note the order of rows in the resulting data frame!**

# COMMAND ----------

display(df.join(labels_df, ['TimeStamp', 'StationId']))

# COMMAND ----------

# MAGIC %md
# MAGIC I recommend not to use the syntax below as it produces extra columns:

# COMMAND ----------

df.join(labels_df, (df.TimeStamp == labels_df.TimeStamp) & (df.StationId == labels_df.StationId)).show()

# COMMAND ----------

df.join(labels_df, ['TimeStamp', 'StationId'], how='left').show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Do not assume any row order if your transformation includes a shuffle!**

# COMMAND ----------

df_left = df.join(labels_df, ['TimeStamp', 'StationId'], how='left')
df_left.show()

# COMMAND ----------

df_left.repartition(2, 'StationId').show()

# COMMAND ----------

# MAGIC %md
# MAGIC You can, of course, sort the results before processing them further.

# COMMAND ----------

displayHTML('<h1 style="color: #009345;">Write code that sorts df_left by StationId AND TimeStamp</h1>')

# COMMAND ----------



# COMMAND ----------

displayHTML('<h1 style="color: #009345;">Write code that makes right join of df and labels_df by StationId AND TimeStamp</h1>')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas User-defined functions
# MAGIC [Pandas user-defined functions](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html) (UDF) allow one to extend Spark with whatever transformations you like in an effecient manner. Spark serializes values and sends Series to your function.
# MAGIC 
# MAGIC **Pandas UDF may be faster sometime than native Spark code!** I experienced this when tried to fill-in NaN values using Spark's Window functions.

# COMMAND ----------

from pyspark.sql.functions import pandas_udf, PandasUDFType

# Use pandas_udf to define a Pandas UDF
@pandas_udf('double', PandasUDFType.SCALAR)
# Input/output are both a pandas.Series of doubles

def pandas_power3(v):
    return v ** 3

df_pdf = df2.withColumn('normal3', pandas_power3(df2.normal))

# COMMAND ----------

df_pdf.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas UDF can take one or several columns as it's input. Imagine, you would like to use function `npower(value, exponent)`, which will rise `value` to the power of `exponent`.

# COMMAND ----------

def npower(value, exponent):
  return value ** exponent

npower(2, 3)

# COMMAND ----------

# Use pandas_udf to define a Pandas UDF
@F.pandas_udf(returnType='double', functionType=PandasUDFType.SCALAR)
# Input/output are both a pandas.Series of doubles
def npower_spark(value, exponent):
    return value ** exponent

# COMMAND ----------

# Define which exponents we would like to use
powers = [2, 3]
# prepare the resulting data frame. This is not necessary. Depends on your use-case.
df2 = df
# Iterate over all exponents
for e in powers:
  # At each step, make a new column
  # Note that we are using F.lit function here
  df2 = df2.withColumn('power{}'.format(e), npower_spark(F.col('value'), F.lit(e)))

# Put all 'power' columns in one list. It might be not necessary here, but illustrates the way how to select columns in more automatic fashion
power_cols = [c for c in df2.columns if c.startswith('power')]
# show the results
df2.select('value', *power_cols).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas UDF can also be applied for grouped data.

# COMMAND ----------

# MAGIC %md
# MAGIC # Window functions
# MAGIC 
# MAGIC Window functions are grouping operations. One performs aggregations on data that falls into window.
# MAGIC 
# MAGIC The difference between group-by and window functions:
# MAGIC 
# MAGIC * Group-by takes data, and every row can go only into one grouping.
# MAGIC * A window function calculates a return value for every input row based on a group of rows.
# MAGIC 
# MAGIC To complicate things more, there are two *windows* in Spark:
# MAGIC 
# MAGIC * `pyspark.sql.Window`, [docs](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.Window).
# MAGIC * `pyspark.sql.functions.window`, [docs](https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html#pyspark.sql.functions.window).
# MAGIC 
# MAGIC The function version is used to work with timestamps. Here, we are going to talk about `pyspark.sql.Window`.
# MAGIC 
# MAGIC One usage example of a window function might be to fill-in NaN/null values with the last known valid value. This operation is known as forward-fill.

# COMMAND ----------

# Let's make a data frame with some null
df_with_nan = df.join(labels_df, ['TimeStamp', 'StationId'], how='left')
df_with_nan.sort('StationId', 'TimeStamp').show()

# COMMAND ----------

window = pyspark.sql.Window.partitionBy('StationId').orderBy('TimeStamp').rowsBetween(pyspark.sql.Window.unboundedPreceding, pyspark.sql.Window.currentRow)
filled_column = F.last(F.col('label'), ignorenulls=True).over(window)
df_filled = df_with_nan.withColumn('label_filled', filled_column)
df_filled.show()

# COMMAND ----------

# MAGIC %md
# MAGIC However, this method will fail misarably on a large data set!

# COMMAND ----------

window = pyspark.sql.Window.orderBy('id').rowsBetween(pyspark.sql.Window.unboundedPreceding, pyspark.sql.Window.currentRow)
df1_filled = df1.withColumn('normal_filled', F.last(F.col('normal'), ignorenulls=True).over(window))
df1_filled.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Pandas UDF will be much faster!

# COMMAND ----------

@F.pandas_udf(returnType='double', functionType=PandasUDFType.SCALAR)
# Input/output are both a pandas.Series of doubles
def pandas_fill(value):
    return value.fillna(method='ffill')
  
df1_filled_pandas = df1.withColumn('normal_filled', pandas_fill('normal'))
df1_filled_pandas.show()
