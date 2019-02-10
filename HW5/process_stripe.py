#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Program to post-process output from buildstripe

import sys
import json

for line in sys.stdin:
    tokens = line.strip().split()
    z = {}
    x = eval(tokens[1])
    for i in x: 
        z.update(i)
    z = {k:int(v) for k, v in z.items()}
    print '%s\t%s' % (tokens[0], z)

        
    