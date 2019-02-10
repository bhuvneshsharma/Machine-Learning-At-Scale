
import sys, os, re
from mrjob.job import MRJob

class MRJoin(MRJob):
  # Data will be sorted by key (the country id), nd then by value:
  # Performs secondary sort on the value starting with either 'A' or 'B'
  SORT_VALUES = True

  def mapper(self, _, line):
    splits = line.rstrip("\n").split("|")

    if len(splits) == 2: # countries
      table = 'A' # make countries sort before transactions data
      cid = splits[1]
      yield cid, (table, splits)
    else: # transactions
      table = 'B'
      cid = splits[2]
      yield cid, (table, splits)

  def reducer(self, key, values):
    country = [None]
    for value in values:
      # country should come first, as countries are sorted on artificial key 'A'. 
      # Also, we assume that country id is a unique identifier
      if value[0] == 'A':
        country=value[1:]
      if value[0] == 'B' and country[0] is not None:
        transaction=value[1:]
        yield key, country + transaction

class MRLeftJoin(MRJoin):
  
  #####################################################################
  # For a Left-Join we want to make sure that we are not emitting any 
  # rows where there is no row in the left table, hence this check: 
  # "and country[0] is not None"
  #####################################################################
  
  
  def reducer(self, key, values):
    
    ##################################################################
    # transactionSeen = False
    #
    # keeps track of whether the transaction has been seen, in other 
    # words, whether there is an entry in the right table 'B'. This 
    # makes the reducer stateful, but only using a single value, 
    # so it is not a memory concern.
    ##################################################################
    
    transactionSeen = False
    
    ##################################################################
    # country = [None]
    #
    # initialize the country to None. Wrap 'None' in a list for  
    # convenience so we can concatenate the country and transaction 
    # lists thus avoiding ugly string manipulation
    ##################################################################
    
    country = [None]
    
    for value in values:
      if value[0] == 'A': 
        country=value[1:]
      if value[0] == 'B' and country[0] is not None: 
        transactionSeen = True
        transaction=value[1:]
        yield key, country + transaction
    if transactionSeen == False and country[0] is not None:
        yield key, country + [None]
    
class MRRightJoin(MRJoin):
  
  #################################################################
  # For a Right-Join we want to make that we are not emitting any 
  # rows where there is no row in the right table, hence this check:
  # we only yield a row if we come across a transaction.
  #################################################################
  
  def reducer(self, key, values):
    country = [None]
    for value in values:
      if value[0] == 'A':
        country=value[1:]
      if value[0] == 'B':
        transaction=value[1:]
        yield key, country + transaction
    
if __name__ == '__main__':
  MRJoin.run()