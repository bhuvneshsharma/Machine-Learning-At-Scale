#!/usr/bin/env python
"""
Reducer aggregates word counts by class and emits frequencies.

INPUT:
    word \t class \t partialCount 
OUTPUT:
    word \t ham_count \t spam_count \t (P(word|ham)) \t P(word|spam)
    
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

import sys

# initialize trackers
cur_word = None
ham_count, spam_count = 0, 0
total_ham_counts, total_spam_counts = 0, 0
partial_ham_counts, partial_spam_counts = 0, 0

# read from standard input
for line in sys.stdin:
    # parse input
    word, isSpam, count = line.split('\t')
    
    # Note: Solution assumes input sorted by word as in Hadoop
    
    # First we read in total counts from mapper (shuffle sort should have aggregated this)
    
    if word == '0ClassPriors':
        if isSpam == '1':
            total_spam_counts += int(count)
        else:
            total_ham_counts += int(count)
    
    # Second we in the partial counts for words
    
    if word == '1PartialCounts':
        if isSpam == '1':
            partial_spam_counts += int(count)
        else:
            partial_ham_counts += int(count)
    
    # Now process rest
    
    if (word != '0ClassPriors') and (word != '1PartialCounts') and (word != '1VocabSize'):
        if word == cur_word:
            if isSpam == '1':
                spam_count += int(count)
            else:
                ham_count += int(count)
        else:
            if cur_word:
                print "%s\t%.1f,%.1f,%.12f,%.12f" % (cur_word, ham_count, spam_count, float(ham_count)/float(partial_ham_counts), float(spam_count)/float(partial_spam_counts))
                
                spam_count, ham_count = 0,0
                
            if isSpam == '1':
                spam_count = int(count)
            else:
                ham_count = int(count)

        cur_word = word

# Final line
print "%s\t%.1f,%.1f,%.12f,%.12f" % (word, ham_count, spam_count, float(ham_count)/float(partial_ham_counts), float(spam_count)/float(partial_spam_counts))

# Print class prior probabilities
print "%s\t%.1f,%.1f,%.12f,%.12f" % ('ClassPriors', total_ham_counts, total_spam_counts, float(total_ham_counts)/(float(total_ham_counts) + float(total_spam_counts)), float(total_spam_counts)/(float(total_ham_counts) + float(total_spam_counts)))
    

        





























##################### (END) CODE HERE ####################