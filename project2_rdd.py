import re
from pyspark import SparkContext, SparkConf
from operator import add
import sys

class project2:
    def run(self, inputPath, outputPath,stopwords,k):
        conf = SparkConf().setAppName("project2")
        sc = SparkContext(conf=conf)

#calculate the year and the term
        def frequency(lis):
            lis = [item for item in lis]
            year=lis[0][0:4]
            for i in range(1,len(lis)-1):
            #only take >a and <z
                if(lis[i][0]>='a' and lis[i][0]<='z'):
                    for j in range(i+1,len(lis)):
                        if(lis[j][0]>='a' and lis[j][0]<='z'):
                            if lis[i]<lis[j]:
                                yield (year,lis[i],lis[j])
                            else:
                                yield (year,lis[j],lis[i])
#take top n
        def topn(lis):
            lis = [item for item in lis]
            year = lis[0]
            for i in range(len(lis[1]) // 2):
                yield year + "\t" + lis[1][2 * i][0] + "," + lis[1][2 * i][1] + ":" + str(lis[1][2 * i + 1])

        file = sc.textFile(inputPath)

        # split the sencence into word
        words = file.map(lambda line: re.split("[\\s,]+", line))
        # stopwords_file = open("/home/comp9313/Downloads/tiny-doc.txt", 'r')
        stopword = sc.textFile(stopwords).flatMap(lambda line: line.split("\n"))
        
        stoplist = stopword.collect()
#remove stopwords
        rdd1 = words.map(lambda x: [item for item in x if item not in stoplist])  # .collect()

        rdd2 = rdd1.flatMap(lambda x: frequency(x))

        out1 = rdd2.map(lambda x: (x, 1))
#count the frequncy
        out2 = out1.reduceByKey(add).sortByKey().sortBy(lambda x: x[1], ascending=False)
#group by year
        out3 = out2.sortBy(lambda x: x[0][0]).map(lambda x: (x[0][0], [x[0][1:], x[1]]))

        top = int(k)
#aggregate into a list,take top n       
        out3 = out3.reduceByKey(add).map(lambda x: [x[0], x[1][0:2 * top]])

        final = out3.flatMap(lambda x: topn(x))

        # .map(lambda x: str(x[0][0]) + "\t" + str(x[0][1])+"," + str(x[0][2])+":"+str(x[1]))

        final.saveAsTextFile(outputPath)
        sc.stop()
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Wrong inputs")
        sys.exit(-1)
    project2().run(sys.argv[1], sys.argv[2],sys.argv[3],sys.argv[4])
