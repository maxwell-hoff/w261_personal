#!/usr/bin/env python
"""
This script reads word counts from STDIN and aggregates
the counts for any duplicated words.

INPUT & OUTPUT FORMAT:
    word \t count
USAGE (standalone):
    python aggregateCounts_v2.py < yourCountsFile.txt

Instructions:
    For Q7 - Your solution should not use a dictionary or store anything   
             other than a single total count - just print them as soon as  
             you've added them. HINT: you've modified the framework script 
             to ensure that the input is alphabetized; how can you 
             use that to your advantage?
"""

# imports
import sys


################# YOUR CODE HERE #################


# failed attempt commented out below:
# #initialize
# prev_word=''
# prev_count=0
# i=0
# # stream over lines from Standard Input
# for line in sys.stdin:
#     # extract words & counts
#     word, count  = line.split()
#     if word != prev_word:
#         print(prev_word, prev_count)
#     prev_word=word
#     prev_count=count
#     if word == prev_word:
#         prev_count+=count
    
    
# stream over lines from Standard Input
for line in sys.stdin:
    # extract words & counts
    word, count  = line.split()
    print(word,count)





################ (END) YOUR CODE #################
