""" wordcount example using the rdd api, we'll write a test for this """
from __future__ import print_function

import sys
from pyspark import SparkContext
import os




# this is the main function which does the word count...
if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        sys.exit("Usage: wordcount file}")
    
    sc = SparkContext(appName="PythonWordCount")
    
    
    #print(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'kit','utils.py'))
    
    # add the utils code to the worker nodes so that they have access to it..    
    sc.addPyFile(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))),'kit','utils.py'))
    
    # since the worker nodes have access to the utils file now it can be imported. 
    from utils import do_word_counts    
    
    lines = sc.textFile(sys.argv[1], 1)
    
    print(do_word_counts(lines))
    

