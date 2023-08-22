from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import sys

class Problem:  

    def run(self, inputPath, outputPath):

        spark = SparkSession.builder.master("local").appName("question2_df").getOrCreate()
        fileDF = spark.read.text(inputPath)
        
        #provide your code here
        
        element = fileDF.select(split(fileDF['value'], ':').getItem(0).alias('shop'), split(fileDF['value'], ':').getItem(1).alias('items'))
        item_df=element.withColumn('item', explode(split('items', ';')))
        item_df1= item_df.select(split(item_df['item'], ',').getItem(0).alias('name'), split(item_df['item'], ',').getItem(1).alias('price'))
        avgDF= item_df1.groupBy('name').agg(max('price')).orderBy('name')
        avgDF.write.format('csv').save(outputPath)
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong arguments")
        sys.exit(-1)
    Problem().run(sys.argv[1], sys.argv[2])
