#!/usr/bin/env python

import os
import sys                                                  
import numpy as np  

#################### YOUR CODE HERE ###################
#enter new parameters for smoothing
# vocab_size = int(os.getenv('vocab_size', default=1))
vocab_size=6
k=1 

class0_word_count=0
class1_word_count=0
ham_doc_count=0
spam_doc_count=0

current_word = None
class0_Count=0
class1_Count=0

# read from standard input
for line in sys.stdin:
    # parse input and tokenize
    line = line.strip()
    partition_key, word, counts = line.split()
    class0_partialCount, class1_partialCount = counts.split(',')
    
#     print(partition_key, word, class0_partialCount, class1_partialCount)
    #check if partial total
    if word == '!class_word_counts':
        class0_word_count+=int(class0_partialCount)
        class1_word_count+=int(class1_partialCount)
    elif word == '!doc_counts_class':
        ham_doc_count+=int(class0_partialCount)
        spam_doc_count+=int(class1_partialCount)
    
    #add to counters if a repeated word
    elif word == current_word:
        class0_Count+=int(class0_partialCount)
        class1_Count+=int(class1_partialCount)
    else:
        #print prior word if it's the last word
        if current_word:
            print(f"{current_word}\t{class0_Count},{class1_Count},{(class0_Count+k)/float(class0_word_count+vocab_size)},{(class1_Count+k)/float(class1_word_count+vocab_size)}")
        current_word = word
        class0_Count=int(class0_partialCount)
        class1_Count=int(class1_partialCount)

#print last record
print(f"{current_word}\t{class0_Count},{class1_Count},{(class0_Count+k)/float(class0_word_count+vocab_size)},{(class1_Count+k)/float(class1_word_count+vocab_size)}")

if int(partition_key) == 0:
    print(f"ClassPriors\t{ham_doc_count},{spam_doc_count},{ham_doc_count/float(ham_doc_count+spam_doc_count)},{spam_doc_count/float(ham_doc_count+spam_doc_count)}")







#################### (END) YOUR CODE ###################