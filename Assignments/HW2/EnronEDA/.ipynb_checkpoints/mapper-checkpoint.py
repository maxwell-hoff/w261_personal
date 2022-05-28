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
from collections import defaultdict

ham_counts = defaultdict(int)
spam_counts = defaultdict(int)

# read from standard input
for line in sys.stdin:
    # parse input
    docID, _class, subject, body = line.split('\t')
    # tokenize
    words = re.findall(r'[a-z]+', subject + ' ' + body)
    
############ YOUR CODE HERE #########
    for word in words:
        if _class == '0':
            ham_counts[word]+=1
        if _class == '1':
            spam_counts[word]+=1

for word, count in ham_counts.items():
    print(f"{word}\t{_class}\t{count}")

for word, count in spam_counts.items():
    print(f"{word}\t{_class}\t{count}")

#     for word in words:
#         print(f"{word}\t{_class}\t{1}")

############ (END) YOUR CODE #########