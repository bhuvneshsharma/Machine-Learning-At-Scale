#!/usr/bin/env python
"""
Reducer to calculate precision and recall as part
of the inference phase of Naive Bayes.
INPUT:
    ID \t true_class \t P(ham|doc) \t P(spam|doc) \t predicted_class
OUTPUT:
    precision \t ##
    recall \t ##
    accuracy \t ##
    F-score \t ##
         
Instructions:
    Complete the missing code to compute these^ four
    evaluation measures for our classification task.
    
    Note: if you have no True Positives you will not 
    be able to compute the F1 score (and maybe not 
    precision/recall). Your code should handle this 
    case appropriately feel free to interpret the 
    "output format" above as a rough suggestion.
"""
import sys

# initialize counters
FP = 0.0 # false positives
FN = 0.0 # false negatives
TP = 0.0 # true positives
TN = 0.0 # true negatives
docs = 0

# read from STDIN
for line in sys.stdin:
    # parse input
    docID, class_, pHam, pSpam, pred = line.split()
    # emit classification results first
    print line[:-2], class_ == pred
    
    # then compute evaluation stats
#################### YOUR CODE HERE ###################
    if class_ == pred:
        if class_ == '1':
            TP += 1.0
        else:
            TN += 1.0
    else:
        if class_== '1':
            FN += 1.0
        else:
            FP += 1.0
    docs += 1

# Now calculate precision, recall, accuracy and print

# Print results
print '# Documents: \t','%d' % (docs)
print 'True Positives: \t','%d' % (TP)
print 'True Negatives: \t','%d' % (TN)
print 'False Positives: \t','%d' % (FP)
print 'False Negatives: \t','%d' % (FN)

if TP == 0:
    print 'Accuracy: \t', '%0.2f' % ((TP+TN)/(TP+TN+FP+FN))
    print 'Recall: \t', '%0.2f' % (TP)
else:
    print 'Accuracy: \t', '%0.4f' % ((TP+TN)/(TP+TN+FP+FN))
    print 'Precision: \t','%0.4f' % (TP/(TP+FP))
    print 'Recall: \t', '%0.4f' % (TP/(TP+FN))
    print 'F1-Score: \t', '%0.4f' % ((2*TP)/(2*TP+FP+FN))    

    
#################### (END) YOUR CODE ###################    