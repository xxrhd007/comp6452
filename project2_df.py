from pyspark.sql.session import SparkSession
from pyspark.sql.functions import  *
import sys
import re
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.window import Window
  


class Project2:
    def run(self, inputPath, outputPath,stopwords,k):

        spark = SparkSession.builder.master("local").appName("project2").getOrCreate()
        dataframe = spark.read.text(inputPath)
        stop = spark.read.text(stopwords)
        top=int(k)

#create a dataframe from a list and remove stopwords
        def create(data, stop):
            stopwords = []
            for i in stop:
                for x in list(i):
                    stopwords.append(x)
            txt = []
            for x in data:
                for line in x:
                    cline = []
                    year = line[0:4]
                    words = line[9:].strip().split(" ")
                    for i in range(len(words) - 1):
                        if words[i] not in stopwords and words[i][0]>='a' and words[i][0]<='z':
                            for j in range(i + 1, len(words)):
                                if words[j] not in stopwords and words[j][0]>='a' and words[j][0]<='z':
                                    if words[i] < words[j]:
                                        cline = [year, words[i] + ',' + words[j]]
                                        txt.append(cline)
                                    else:
                                        cline = [year, words[j] + ',' + words[i]]
                                        txt.append(cline)
            return txt
#create dataframe,two column:year,term
        data2 = create(dataframe.collect(), stop.collect())
        schema = StructType([ \
            StructField("year", StringType(), True), \
            StructField("term", StringType(), True), \
            ])

        tf = spark.createDataFrame(data=data2, schema=schema)
#calculate frequncy
        countTF = tf.groupBy("year", "term").count()
#orderby year and count
        out = countTF.orderBy("year", desc("count"))

#using window function,partition year
        windowDept = Window.partitionBy("year").orderBy(col("count").desc(), col("term"))
#count row number group by year
        out1 = out.withColumn("row", row_number().over(windowDept))
        out3 = out1.filter(col("row") <=top ).drop("row")
#turn count into string and output
        final = out3.withColumn("count", out3["count"].cast(StringType())).select(concat(col("year"), lit("\t"), col("term"),lit(":"),col("count")))
        final.write.text(outputPath)
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    Project2().run(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])
