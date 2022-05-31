#!/usr/bin/env python
"""
Mapper reads in text documents and emits word counts by class.
INPUT:                                                    
    DocID \t true_class \t subject \t body                
OUTPUT:                                                   
    partitionKey \t word \t class0_partialCount,class1_partialCount       
    

Instructions:
    You know what this script should do, go for it!
    (As a favor to the graders, please comment your code clearly!)
    
    A few reminders:
    1) To make sure your results match ours please be sure
       to use the same tokenizing that we have provided in
       all the other jobs:
         words = re.findall(r'[a-z]+', text-to-tokenize.lower())
         
    2) Don't forget to handle the various "totals" that you need
       for your conditional probabilities and class priors.
       
Partitioning:
    In order to send the totals to each reducer, we need to implement
    a custom partitioning strategy.
    
    We will generate a list of keys based on the number of reduce tasks 
    that we read in from the environment configuration of our job.
    
    We'll prepend the partition key by hashing the word and selecting the
    appropriate key from our list. This will end up partitioning our data
    as if we'd used the word as the partition key - that's how it worked
    for the single reducer implementation. This is not necessarily "good",
    as our data could be very skewed. However, in practice, for this
    exercise it works well. The next step would be to generate a file of
    partition split points based on the distribution as we've seen in 
    previous exercises.
    
    Now that we have a list of partition keys, we can send the totals to 
    each reducer by prepending each of the keys to each total.
       
"""

import re                                                   
import sys                                                  
import numpy as np      

from operator import itemgetter
import os
from collections import defaultdict

#################### YOUR CODE HERE ###################
#read number of partitions specified by mapreduce job
N = int(os.getenv('mapreduce_job_reduces', default=1))

# helper functions
def makeKeyHash(key, num_reducers=N):
    byteof = lambda char: int(format(ord(char), 'b'), 2)
    current_hash = 0
    for c in key:
        current_hash = (current_hash * 31 + byteof(c))
    return current_hash % num_reducers

# helper function
def makeKeyFile(num_reducers=N):  
    KEYS = list(map(chr, range(ord('A'), ord('Z')+1)))[:N]
    partition_keys = sorted(KEYS, key=lambda k: makeKeyHash(k,num_reducers=N))

    return partition_keys


# call your helper function to get partition keys
pKeys = makeKeyFile()

# initialize counters/vars
# doc_counter=0
ham_doc_count=0
spam_doc_count=0

ham_word_count=0
spam_word_count=0

unique_words={}



# ham_counts = defaultdict(int)
# spam_counts = defaultdict(int)

# read from standard input
for line in sys.stdin:
    # parse input and tokenize
    docID, _class, subject, body = line.lower().split('\t')
    words = re.findall(r'[a-z]+', subject + ' ' + body)
    
    #version 1
    #create counters for total documents and documents by class
#     doc_counter+=1
    if _class == '0':
        ham_doc_count+=1
    elif _class == '1':
        spam_doc_count+=1
    
    #loop through each word
    for word in words:
        class0_partialCount=0
        class1_partialCount=0
        pKey = pKeys[makeKeyHash(word)]
        unique_words[word]=''
        if _class == '0':
            class0_partialCount+=1
            ham_word_count+=1
        elif _class == '1':
            class1_partialCount+=1
            spam_word_count+=1
        

#     #version 2 - using a dictionary
#     #create counters for total documents and documents by class
#     doc_counter+=1
#     if _class == '0':
#         ham_counter+=1
#     elif _class == '1':
#         spam_counter+=1
    
#     # initialize counters 

#     for word in words:
#         if _class == '0':
#             ham_counts[word]+=1
#         elif _class == '1':
#             spam_counts[word]+=1









        print(f"{pKey}\t{word}\t{class0_partialCount}\t{class1_partialCount}")
for key in pKeys:
#     print(f"{pKey}\tClassPriors\t{ham_counter}\t{spam_counter}\t{doc_counter}")
    print(f"{key}\t!class_word_counts\t{ham_word_count}\t{spam_word_count}")
    print(f"{key}\t!distinct_words\t{len(unique_words)}\t0")
    print(f"{key}\t!doc_counts_class\t{ham_doc_count}\t{spam_doc_count}")
# print(f"Total\tdoc_counts\t{doc_counter}\t0")








#################### (END) YOUR CODE ###################