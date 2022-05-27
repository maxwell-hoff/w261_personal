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



# version 3
current_word = word
if is_spam == '1':
   spam_count, ham_count = int(count), 0
else:
   spam_count, ham_count = 0, int(count)



#     # tally counts from current key
#     if key == cur_word: 
#         cur_count += int(count)
#     # OR ...  
#     else:
#         # store word count total
#         if cur_word == 'total':   # part b/c - UNCOMMENT & MAKE YOUR CHANGE HERE    

#             total = float(cur_count)  
#         # emit realtive frequency
#         if cur_word:
#             print(f'{cur_word}\t{cur_count/total}')
#         # and start a new tally 
#         cur_word, cur_count  = key, int(value)

# # don't forget the last record! 
# print(f'{cur_word}\t{cur_count/total}')



#old version
#     if current_word == None:
#         if is_spam == "0":
#             ham_count+=1
#         if is_spam == "1":
#             spam_count+=1
#     if word == current_word:
#         if is_spam == "0":
#             ham_count+=1
#         if is_spam == "1":
#             spam_count+=1
#     else:
#         for x in is_spam:
#             print(f"{current_word}\t0\t{ham_count}")
#             print(f"{current_word}\t1\t{spam_count}")
#             if is_spam == "0":
#                 ham_count+=1
#             if is_spam == "1":
#                 spam_count+=1
#     current_word = word

############ (END) YOUR CODE #########