from pyspark import SparkContext
import itertools
 
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('/user/estrella.moreira/output_test0517/big_box_grocers/part-00000-af3cb71f-91aa-413f-a156-b57336618098-c000.csv')
    header = rdd.first()
    rdd.sample(False, 0.01) \
        .coalesce(1) \
        .mapPartitions(lambda x: itertools.chain([header], x)) \
        .saveAsTextFile('big_box_grocers')
