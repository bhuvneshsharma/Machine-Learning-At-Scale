#!/usr/bin/env python
"""
Mapper reads model and emits words their conditonal probability (for spam) and assigns them a class/partition depending on value

INPUT:
    word \t hamCount,spamCount,pHam,pSpam

OUTPUT:
    pSpam \t word \t partition
"""

max = 0.0029726581998 # Max probability; got this by iterating through once

import sys
for line in sys.stdin:
    word, payload = line.split()
    spam_cProb = payload.strip().split(',')[3]
    
    # Ideally we want to split the partition by distribution but we won't know that at first read so have to manually adjust
    #if max <= float(spam_cProb):
    #    max = float(spam_cProb)
    
    if float(spam_cProb) >= 0.1*max:
        print "%s\t%s\t%s" % (spam_cProb, word , 'A')
    elif float(spam_cProb) >= 0.08*max and float(spam_cProb) < 0.1*max:
        print "%s\t%s\t%s" % (spam_cProb, word , 'B')
    elif float(spam_cProb) >= 0.05*max and float(spam_cProb) < 0.08*max:            
        print "%s\t%s\t%s" % (spam_cProb, word , 'C')
    elif float(spam_cProb) >= 0.02*max and float(spam_cProb) < 0.05*max:  
        print "%s\t%s\t%s" % (spam_cProb, word , 'D')
    else:             
        print "%s\t%s\t%s" % (spam_cProb, word , 'E')

# Used once        
#print max
                                                                                                                               