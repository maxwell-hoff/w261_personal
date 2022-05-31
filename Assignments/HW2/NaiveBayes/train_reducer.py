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
distinct_words=0
ham_doc_count=0
spam_doc_count=0

# # read from standard input
# for line in sys.stdin:
#     # parse input and tokenize
#     partition_key, word, class0_partialCount, class1_partialCount = line.lower().split()
#     if word == 'class_word_counts':
#         class0_word_count+=class0_partialCount
#         class1_word_count+=class1_partialCount
#     if word == 'distinct_words':
#         distinct_words+=class0_partialCount
#     if word == 'doc_counts_class':
#         ham_doc_count+=class0_partialCount
#         spam_doc_count+=class1_partialCount

current_word = None
class0_Count=0
class1_Count=0
class0_condprob=0
class1_condprob=0

# read from standard input
for line in sys.stdin:
    # parse input and tokenize
    partition_key, word, class0_partialCount, class1_partialCount = line.lower().split()
#     print(partition_key, word, class0_partialCount, class1_partialCount)
    if word == '!class_word_counts':
        class0_word_count+=float(class0_partialCount)
        class1_word_count+=float(class1_partialCount)
    if word == '!distinct_words':
        distinct_words+=float(class0_partialCount)
    if word == '!doc_counts_class':
        ham_doc_count+=float(class0_partialCount)
        spam_doc_count+=float(class1_partialCount)
    if word == current_word:
        if class0_partialCount == '1':
            class0_Count+=float(class0_partialCount)
            class0_condprob = float(class0_Count+1)/(float(class0_word_count+0e-6)+float(distinct_words+0e-6))
        elif class1_partialCount == '1':
            class1_Count+=float(class1_partialCount)
            class1_condprob = float(class1_Count+1)/(float(class1_word_count)+float(distinct_words))
    else:
        if current_word and not word.startswith('!'):
            if class1_Count>0 or class0_Count>0:
                print(f"{current_word}\t{class0_Count},{class1_Count},{class0_condprob},{class1_condprob}")
        if not word.startswith("!"):
            current_word = word
            class0_Count=0
            class1_Count=0
            class0_condprob=0
            class1_condprob=0
            if class0_partialCount == '1':
                class0_Count+=float(class0_partialCount)
            elif class1_partialCount == '1':
                class1_Count+=float(class1_partialCount)



print(f"{current_word}\t{class0_Count},{class1_Count},{class0_condprob},{class1_condprob}")
print(f"ClassPriors\t")






##################### (END) CODE HERE ####################