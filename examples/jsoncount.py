""" wordcount example using the rdd api, we'll write a test for this """
from __future__ import print_function

import sys
import os
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType



if __name__ == "__main__":
    
    
    sc = SparkContext(appName="PythonJsonCount")
    
    
    spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
    
    
    schema1 = StructType([StructField("name", StringType(), True)])


    df = spark.read.json(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'data','sample.json'), schema1)
    
    
    print(df.show())
    
    # add the utils code to the worker nodes so that they have access to it..    
    sc.addPyFile(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'kit','utils.py'))
    
    # since the worker nodes have access to the utils file now it can be imported. 
    from utils import do_json_counts    
    
    print("Name vikas found %d times" % do_json_counts(df, 'vikas'))
    