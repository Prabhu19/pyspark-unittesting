# -*- coding: utf-8 -*-

from operator import add



# Function 1 
# Perform the word count and return the result...
def do_word_counts(lines):
    """ count of words in an rdd of lines """

    counts = (lines.flatMap(lambda x: x.split())
                  .map(lambda x: (x, 1))
                  .reduceByKey(add)
             ) 
    results = {word: count for word, count in counts.collect()}
    return results
    
    

# Function 2 
# count of records where name=target_name in a dataframe with column 'name'
def do_json_counts(df, target_name):
    """ count of records where name=target_name in a dataframe with column 'name' """

    return df.filter(df.name == target_name).count()