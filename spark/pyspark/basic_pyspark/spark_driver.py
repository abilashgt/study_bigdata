import sys
import findspark

# spark init
findspark.init()
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkContext('local', 'Exam_3')
sqlc = SQLContext(sc)

# test python
print("test python")

# test spark
rdd = sc.parallelize(['test','spark'])
print(rdd.collect())  # print

# test rdd


sys.exit()
