from pyspark.sql.session import SparkSession
from pyspark.sql.functions import  *
import sys
import re
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark import SparkContext, SparkConf


inputPath=sys.argv[1]
outPath=sys.argv[2]
th=float(sys.argv[3])

spark = SparkSession.builder.master("local").appName("project3").getOrCreate()
schema = StructType([StructField("id", IntegerType(), False), StructField("hl", StringType(), False)])
file_rdd = spark.sparkContext.textFile(inputPath).zipWithIndex()



rdd_row = file_rdd.map(lambda x:(x[1], x[0]))
initial_df = spark.createDataFrame(rdd_row, schema)
initital_df=initial_df.filter("hl is not NULL")


#select year from initial dataframe
initial_df = initial_df.withColumn("years", substring(col("hl"), 0, 4).cast("Int"))#.withColumnRenamed("id","year_id")#.drop("hl")


#explode headline into single word
hl_set = initial_df.withColumn("hl", split(substring_index(col("hl"), ",", -1), " "))
word_freq = hl_set.withColumn("word", explode(col("hl"))).filter(length(col("word")) > 0).groupBy("word").count().sort("count")
int_words=word_freq.withColumn('v', monotonically_increasing_id())
word_list=hl_set.withColumn("w", explode(col("hl"))).filter(length(col("w")) > 0)


#convert headline into int
int_words2 = word_list.join(int_words, int_words.word == word_list.w,"inner").select("id","v","years")
int_hl = int_words2.groupBy("id","years").agg(collect_set(col("v")).alias("v_set")).withColumn("v_set", expr("array_sort(v_set,(l, r) -> case when l < r then -1 when l > r then 1 else 0 end)"))
int_hl1= int_hl.selectExpr("id as id2","years as years2","v_set as v_set2")


#filter prefix tokens
add_length= int_hl.withColumn("length", size(col("v_set")).cast("Byte")).withColumn("range", sequence(lit(1), col("length")))
range_hl= add_length.withColumn("v_set", arrays_zip(col("range"), col("v_set")))
prefix_tokens1 = range_hl.withColumn("v", explode(col("v_set")))
prefix_tokens=prefix_tokens1.withColumn("index", prefix_tokens1.v.range).withColumn("v", prefix_tokens1.v.v_set).drop("v_set","range")
filterd_tokens = prefix_tokens.filter(col("index") <= (col("length") - ceil(col("length") * th)+1))
filterd_tokens1=filterd_tokens.selectExpr("id as id1", "index as index1","years as years1", "length as length1", "v as v1")


#generate candidate pairs,filter by length
candidate_pairs = filterd_tokens.alias("df1").join(filterd_tokens1.alias("df2"), (col("df1.v") == col("df2.v1"))&(col("df1.id") < col("df2.id1"))&(col("df1.years") != col("df2.years1"))&(floor(col("df1.length") * th) <= col("df2.length1"))&(col("df2.length1") <= ceil(col("df1.length") / th))).dropDuplicates(["id","id1"]).filter(col("df1.id")<col("df2.id1")).select( "df1.id","df1.v","df1.index","df1.years","df2.id1","df2.v1","df2.index1","df2.years1")


#join headline
hl_pairs = candidate_pairs.alias("df1").join(int_hl.alias("df2"), col("df1.id") == col("df2.id")).drop("hl").select("df1.id", "df1.years", "df2.v_set","df1.id1", "df1.years1").withColumnRenamed('v_set','set')
hl_pairs = hl_pairs.alias("df1").join(int_hl.alias("df2"), col("df1.id1") == col("df2.id")).select("df1.id","df1.years", "set","df1.id1","df1.years1","df2.v_set").withColumnRenamed('v_set','set1')


#calculate simlilarity
similarities = hl_pairs.withColumn("sim", size(array_intersect(col("set"), col("set1"))).cast("Double") / size(array_union(col("set"), col("set1")))).sort("id", "id1").filter(col("sim")>=th)
final = similarities.withColumn("sim", col("sim").cast(StringType())).select(concat(lit("("),col("id"), lit(","), col("id1"),lit(")"),lit("\t"),col("sim")))

final.write.text(outPath)
spark.stop()

