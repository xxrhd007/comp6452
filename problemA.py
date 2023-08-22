from pyspark import SparkContext, SparkConf
import sys
from operator import avg
class Problem:  

    def run(self, inputPath, outputPath):

        conf = SparkConf().setAppName("question2_rdd")
        sc = SparkContext(conf=conf)
        fileRDD = sc.textFile(inputPath)

        #provide your code here
        rdd1=fileRDD.map(lambda x: x.split(" "))
        rdd2=rdd1.map(lambda x:((x[0],x[1]),float(x[2])))
        rdd3.rdd2.reduceByKey(lambda x:x[1]).count()
       
        sc.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Wrong arguments")
        sys.exit(-1)
    Problem().run(sys.argv[1], sys.argv[2])
