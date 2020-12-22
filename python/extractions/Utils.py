#!/usr/bin/env python
# coding: utf-8

# 

# In[6]:


class Utils:
    
    def pool_partitioner(self,batchSize,intervalStart,intervalEnd):
        elementSize = (intervalEnd - intervalStart)
        division = -(-elementSize // batchSize)
        splitter = list()
        for i in range(1,division+1):
            if i==1:
                sp = intervalStart
            else:
                sp=(batchSize*(i-1))+intervalStart

            if division == i:
                ep = intervalEnd + 1
            else:
                ep=(i*batchSize) + intervalStart
            ranger = range(sp,ep)
            splitter.append(ranger)
        return splitter

