#!/usr/bin/env python
"""
Reducer script to count unique words.
INPUT:
    word \t 1  (sorted alphabetically)
OUTPUT:
    an integer count
"""
import re
import sys

cur_word = None
word_count = 0
# read from standard input
for line in sys.stdin:
    line = line.strip()

############ YOUR CODE HERE #########
    if line != cur_word:                  # <--- SOLUTION --->
        word_count += 1                   # <--- SOLUTION --->
        cur_word = line                   # <--- SOLUTION --->
print(f"NumUniqueWords\t{word_count}")    # <--- SOLUTION --->
############ (END) YOUR CODE #########
