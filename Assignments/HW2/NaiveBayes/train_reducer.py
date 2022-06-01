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
    if word == '!class_word_counts':
        class0_word_count+=float(class0_partialCount)
        class1_word_count+=float(class1_partialCount)
    elif word == '!doc_counts_class':
        ham_doc_count+=float(class0_partialCount)
        spam_doc_count+=float(class1_partialCount)
    elif word == current_word:
        class0_Count+=float(class0_partialCount)
        class1_Count+=float(class1_partialCount)
#         if class0_partialCount == '1':
#             class0_Count+=float(class0_partialCount)
#             if class0_word_count == 0:
#                 class0_condprob = 0
#             else:
#                 class0_condprob = float(class0_Count)/(float(class0_word_count))
#         elif class1_partialCount == '1':
#             class1_Count+=float(class1_partialCount)
#             if class1_word_count == 0:
#                 class1_word_count = 0
#             else:
#                 class1_condprob = float(class1_Count)/(float(class1_word_count))
    else:
        if current_word:
            print(f"{current_word}\t{class0_Count},{class1_Count}\t{class0_Count/class0_word_count}\t{class1_Count/class1_word_count}")
#             if class1_Count>0 or class0_Count>0:
#                 print(f"{current_word}\t{class0_Count},{class1_Count},{class0_word_count},{class1_word_count},{class0_condprob},{class1_condprob}")
#         if not word.startswith("!"):
        current_word = word
        class0_Count=float(class0_partialCount)
        class1_Count=float(class1_partialCount)

#             if class0_partialCount == '1':
#                 class0_Count+=float(class0_partialCount)
#                 if class0_word_count == 0:
#                     class0_word_count = 0
#                 else:
#                     class0_condprob = float(class0_Count)/(float(class0_word_count))
#             elif class1_partialCount == '1':
#                 class1_Count+=float(class1_partialCount)
#                 if class1_word_count == 0:
#                     class1_word_count = 0
#                 else:
#                     class1_condprob = float(class1_Count)/(float(class1_word_count))



print(f"{current_word}\t{class0_Count},{class1_Count}\t{class0_Count/class0_word_count}\t{class1_Count/class1_word_count}")

if int(partition_key) == 0:
    print(f"ClassPriors\t{ham_doc_count},{spam_doc_count},{ham_doc_count/float(ham_doc_count+spam_doc_count)},{spam_doc_count/float(ham_doc_count+spam_doc_count)}")





##################### (END) CODE HERE ####################