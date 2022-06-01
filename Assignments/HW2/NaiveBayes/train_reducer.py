#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    partitionKey \t word \t class0_partialCount,class1_partialCount 
OUTPUT:
    word \t class0_totalCount,class1_totalCount,class0_condprob,class1_condprob
    
Instructions:
    Again, you are free to design a solution however you see 
    fit as long as your final model meets our required format
    for the inference job we designed in Question 8. Please
    comment your code clearly and concisely.
    
    A few reminders: 
    1) Don't forget to emit Class Priors (with the right key).
    2) In python2: 3/4 = 0 and 3/float(4) = 0.75
"""
##################### YOUR CODE HERE ####################

import re
import sys

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
            print(f"{current_word}\t{class0_Count},{class1_Count},{class0_Count/float(class0_word_count)},{class1_Count/float(class1_word_count)}")
        current_word = word
        class0_Count=int(class0_partialCount)
        class1_Count=int(class1_partialCount)

#print last record
print(f"{current_word}\t{class0_Count},{class1_Count},{class0_Count/float(class0_word_count)},{class1_Count/float(class1_word_count)}")

if int(partition_key) == 0:
    print(f"ClassPriors\t{ham_doc_count},{spam_doc_count},{ham_doc_count/float(ham_doc_count+spam_doc_count)},{spam_doc_count/float(ham_doc_count+spam_doc_count)}")





##################### (END) CODE HERE ####################