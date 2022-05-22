#!/usr/bin/env python
"""
Reducer takes words with their class and partial counts and computes totals.
INPUT:
    word \t class \t partialCount 
OUTPUT:
    word \t class \t totalCount  
"""
import re
import sys

# initialize trackers
current_word = None
spam_count, ham_count = 0,0

# read from standard input
for line in sys.stdin:
    # parse input
    word, is_spam, count = line.split('\t')
    
############ YOUR CODE HERE #########
    if current_word == None:
        if is_spam == "0":
            ham_count+=1
        if is_spam == "1":
            spam_count+=1
    if word == current_word:
        if is_spam == "0":
            ham_count+=1
        if is_spam == "1":
            spam_count+=1
    else:
        for x in is_spam:
            print(f"{current_word}\t0\t{ham_count}")
            print(f"{current_word}\t1\t{spam_count}")
            if is_spam == "0":
                ham_count+=1
            if is_spam == "1":
                spam_count+=1
    current_word = word

############ (END) YOUR CODE #########