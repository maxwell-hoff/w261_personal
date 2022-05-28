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
from collections import defaultdict

ham_counts = defaultdict(int)
spam_counts = defaultdict(int)

# initialize trackers
current_word = None
spam_count, ham_count = 0,0

# read from standard input
for line in sys.stdin:
    # parse input
    word, is_spam, count = line.split('\t')
    
############ YOUR CODE HERE #########
    # create counts for each class
    if is_spam == '0':
        ham_count[word] += int(count)
    if is_spam == '1':
        spam_count[word] += int(count)

# print resutls  
for wrd, cnt in ham_count.items():
    print(f"{wrd}\t{0}\t{cnt}")
for wrd, cnt in spam_count.items():
    print(f"{wrd}\t{1}\t{cnt}")
        


############ (END) YOUR CODE #########