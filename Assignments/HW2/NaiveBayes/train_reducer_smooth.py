#!/usr/bin/env python

import os
import sys                                                  
import numpy as np  

#################### YOUR CODE HERE ###################



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
            class0_condprob = float(class0_Count+1)/(float(class0_word_count)+float(distinct_words))
        elif class1_partialCount == '1':
            class1_Count+=float(class1_partialCount)
            class1_condprob = float(class1_Count+1)/(float(class1_word_count)+float(distinct_words))
    else:
        if current_word and not word.startswith('!'):
            if class1_Count>0 or class0_Count>0:
                print(f"{current_word}\t{class0_Count},{class1_Count},{class0_word_count},{class1_word_count},{class0_condprob},{class1_condprob}")
        if not word.startswith("!"):
            current_word = word
            class0_Count=0
            class1_Count=0
            class0_condprob=0
            class1_condprob=0
            class0_word_count=0
            class1_word_count=0
            if class0_partialCount == '1':
                class0_Count+=float(class0_partialCount)
                class0_condprob = float(class0_Count+1)/(float(class0_word_count)+float(distinct_words))
            elif class1_partialCount == '1':
                class1_Count+=float(class1_partialCount)
                class1_condprob = float(class1_Count+1)/(float(class1_word_count)+float(distinct_words))



print(f"{current_word}\t{class0_Count},{class1_Count},{class0_word_count},{class1_word_count},{class0_condprob},{class1_condprob}")
print(f"ClassPriors\t")

































#################### (END) YOUR CODE ###################