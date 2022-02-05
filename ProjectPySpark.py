import pyspark
from pyspark.sql.functions import count,col
from pyspark.sql import SparkSession




#loading data
movie = spark.read.options(header='True', inferSchema='True', delimiter=',').csv("G:\Project SPARK\Movies_Data.csv")




-----------------------------------------------------------------------------
#Directors who were nominated and have won awards in the year 2011
df1 = movie.filter("year = 2011").filter(movie.outcome.contains("Won")).show()
df2 = movie.filter("year = 2011").filter(movie.outcome.contains("Nominated")).show()
using Union

First 20
 records and for Won
unionDF = df1.union(df2)
unionDF.show(truncate=False)

Last 20 records and for Nominated
x = uniondf.tail(20)
df3 = sqlContext.createDataFrame(x)
df3.show(truncate=False)

using Join
movieDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner") \
     .show(truncate=False)
MovieDF.join(df1 == df2).show()
-----------------------------------------------------------------------------------

#Award categories available in the Berlin International Film Festival
movie.filter(movie.ceremony == 'Berlin International Film Festival').show(truncate=False)



-----------------------------------------------------------------------------------------------


#Directors who won awards for making movies in French

movie.filter( (movie.outcome  == "Won") & (movie.original_language  == "fi") ).show(truncate=False)




---------------------------------------------------------------------------------------------------------




#Directors who have won awards more than 10 times

#df=movie.groupBy("director_name").count()
#df.show()
df=movie.filter(movie.outcome.contains("Won")).groupBy("director_name").count()
df.filter("count > 10").show()


-----------------------------------------------------------------------------------------------------------




#Year wise ascending for oldest movie--

x = movie.sort(movie.year.asc()).take(1)
df2 = sqlContext.createDataFrame(x)
df2.show(truncate=False)

#Year wise Descending for newest movie--
y = movie.sort(movie.year.desc()).take(1)
df3 = sqlContext.createDataFrame(y)
df3.show(truncate=False)


#Most Awarded directors name
df = df.withColumnRenamed("count","Outcome1")
x=df.sort(df.Outcome1.desc()).show(truncate=False)


-   -    -    -    - -    -    -    -   -    -  -   - -    - - - - -  -- -  - -  - - -
df = df.withColumnRenamed("count","Outcome1")
#df.show(truncate=False)




-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------








