#!/usr/bin/env python
"""
Mapper tokenizes and emits words with their class.
INPUT:
    ID \t SPAM \t SUBJECT \t CONTENT \n
OUTPUT:
    word \t class \t count 
"""
import re
import sys

# read from standard input
for line in sys.stdin:
    # parse input
    docID, _class, subject, body = line.split('\t')
    # tokenize
    words = re.findall(r'[a-z]+', subject + ' ' + body)
    
############ YOUR CODE HERE #########
    for word in words:
        count=1
        print(f"{word}\t{_class}\t{count}")

############ (END) YOUR CODE #########