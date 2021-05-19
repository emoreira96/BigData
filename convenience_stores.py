from pyspark import SparkContext
import itertools
 
if __name__=='__main__':
    sc = SparkContext()
    rdd = sc.textFile('/user/estrella.moreira/output_test0517/convenience_stores/part-00000-bf2db064-ac2c-4b9d-b99a-0eaec7bbe6ea-c000.csv')
    header = rdd.first()
    rdd.sample(False, 0.01) \
        .coalesce(1) \
        .mapPartitions(lambda x: itertools.chain([header], x)) \
        .saveAsTextFile('big_box_grocers')
