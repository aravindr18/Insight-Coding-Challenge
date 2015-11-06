# program that constructs graphs from hashtags within the 60 seconds window
#!/usr/bin/env python

from __future__ import division
import sys
import simplejson as json
import pandas as pd
import numpy as np
import re
from datetime import datetime
from collections import defaultdict
import itertools
import time


class Graph(object):
    # Graph data structure, undirected by default
    def __init__(self, connections, directed=False):
        self._graph = defaultdict(set)
        self._directed = directed
        self.add_connections(connections)

    def __iter__(self):
        return self
    
    def add_connections(self, connections):
        #Add connections (list of tuple pairs) to graph
        
        for node1, node2 in connections:
            self.add(node1, node2)

    def add(self, node1, node2):
        #Add connection between node1 and node2 

        self._graph[node1].add(node2)
        if not self._directed:
            self._graph[node2].add(node1)

def set_date(date_str):
    #Convert string to datetime
    time_struct = time.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y") #Tue Apr 21 10:05:15 +0000 2015
    date = datetime.fromtimestamp(time.mktime(time_struct))
    return date

def average_degree(g):
    count =0
    numofNodes=0
    for i in g._graph.keys():
        numofNodes+=1
        count+=len(list(g._graph[i]))
    try:
        number = "{0:.2f}".format(float(count/numofNodes))
    except ZeroDivisionError:
        number = "{0:.2f}".format(0)
    return number

def extract_json(file_path):
    contents = open(file_path, "r").read() 
    data = np.array([json.loads(str(item)) for item in contents.strip().split('\n') if item.strip()])
    return data

def filter_json(data):
    hashtags_extract = []
    timestamp = []
    count=0
    # extract timestamp and hashtags and store in a list
    for i in data:   
        try:
            timestamp.append(i['created_at'])
        except KeyError:
            continue
        temp2 = []
        if 'entities' in i:
            if 'hashtags' in i['entities']:            
                for j in i['entities']['hashtags']:
                    cleaned = re.sub('[^\x00-\x7F]','',j['text']).lower()
                    temp2.append(cleaned)
                hashtags_extract.append(temp2)
            else:
                hashtags_extract.append([])
        else:
            hashtags_extract.append([])

    df = pd.DataFrame(data = zip(np.asarray(hashtags_extract).transpose(),
                            np.asarray(timestamp).transpose()),
                            columns=['hashtags','timestamp'])
    return (df,hashtags_extract,timestamp)
    
    

def slidingWindow(df,hashtags_extract,timestamp,outfile):
    counter = 0
    idx=0
    # live_nodes would be the node that contains all the hashtags as a list of list [[]]
    live_nodes = []
    
    for i in df['timestamp']:
        # iterate row by row in the dataframe
        
        if counter ==0:
            # first tweet in the input file         
            live_nodes.append(df['hashtags'][counter])
            
            time1 = set_date(i) # Starting point of 60seconds window
            time2 = set_date(i) # Ending point of 60seconds window
            
            # this guy does some awesome list comprehensions to find combinations between hastags
            # if the input is [['trump','election','news']] then connections will 
            # output [[('trump','election'),('election','news'),('news','trump')]
            connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
            # we then feed the connection list to the graph class to create a graph
            g = Graph(sum(connections,[])) # flatten the connections list by sum(conenctions,[])
            a = average_degree(g)# compute average dergree 
            file_write(a,outfile)
            
        else:
            # from 2nd row onwards
            time2 = set_date(i)   # ending point of the 60 seconds window
            # are the two tweet timestamps within the 60seconds timeframe
            if (time2-time1).seconds <=60:
                
                live_nodes.append(df['hashtags'][counter])
                connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
                g = Graph(sum(connections,[]))
                a = average_degree(g)
                file_write(a,outfile)
                               
            else:
                # delete the hashtag that does not fall in this window
                temp = hashtags_extract[idx]
                # index to figure out the starting point of my window
                idx +=1
                time1 =set_date(timestamp[idx])

                if temp!=[]:
                    live_nodes.remove(temp)            
                live_nodes.append(df['hashtags'][counter])

                connections = [[x for x in itertools.combinations(a,2)] for a in live_nodes]
                g = Graph(sum(connections,[]))
                a = average_degree(g)

                file_write(a,outfile)

        counter+=1

    
def file_write(a,outfile):
    f = open(outfile,'a')
    f.write(str(a) + "\n")
    f.close()

def main(argv):
    start_time = time.time()
    filepath = argv[1]
    outfile = argv[2]
    tweet_data = extract_json(filepath)
    
    print "Tweets extracted from JSON. The process took " + str(time.time()-start_time) + " seconds"
    (df,hashtags_extract,timestamp) = filter_json(tweet_data)
    print "Constructed dataframe. The process took " + str(time.time()-start_time)  + " seconds"
    print "Tweets being processed "
    slidingWindow(df,hashtags_extract,timestamp,argv[2])
    print "Output file created at " + argv[2]
    print "Execution took " + str(time.time()-start_time) + " seconds"

if __name__ == "__main__":
    main(sys.argv)