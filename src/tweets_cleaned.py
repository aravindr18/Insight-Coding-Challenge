# example of program that calculates the number of tweets cleaned
#!/usr/bin/env python

# importing the necessary module
import sys
import simplejson as json
import pandas as pd
import numpy as np
import re
import time

# this function reads a json file and returns a numpy array 
def extract_json(file_path):
    contents = open(file_path, "r").read() 
    data = np.array([json.loads(str(item)) for item in contents.strip().split('\n') if item.strip()])
    return data
    
# extract the text and timestamp from the json and return a pandas dataframe
def filter_json(data):
    
    
    
    list1= [] # list to hold the texts
    list2= [] # list to hold the timestamp
    for i in data:
        try:   # ensure that the json contains the "text" field
            list1.append(i['text'])
            list2.append(i['created_at'])
        except KeyError:
            continue
    # Now with text and timestamp extracted, let us create a numpy column vector
    # this would be easier for directly adding to our pandas dataframe
    text = np.asarray(list1).transpose()
    created_at = np.asarray(list2).transpose()
       
    df = pd.DataFrame(data = zip(text,created_at), columns=['text','created_at'])
    
    return df


# this function counts the number of unicode tweets and returns a integer number
def count_unicodeTweets(df):
    tweet_count =0
    
    for i in df['text']:
        if bool(re.search('[^\x00-\x7F]', i)):
            tweet_count+=1
    return tweet_count
    

# Now the main thing - Clean the tweets
def filterTweets(df):
    df['text']= df.text.str.replace('[^\x00-\x7F]','')
    df['text'] = df.text.str.replace('\n','')



# save the output to a file 
def to_file(df, filename,count):
    f = open(filename, 'w')
    for col in df.values:
        f.write(col[0] +" (timestamp: ")
        f.write(col[1] + ") \n")
        
    f.write(str(count) + " tweets contained unicode")
    f.close()
    
    
# main function here. To run this script use python tweets_cleaned.py <input_filepath> <output_filepath>
# relative filepaths need to be given 

def main(argv):
    start_time = time.time()
    
    filepath = argv[1]
    tweet_data = extract_json(filepath)
    
    print "Tweets extracted from JSON. The process took " + str(time.time()-start_time) + " seconds"
    df = filter_json(tweet_data)
    print "Constructed dataframe. The process took " + str(time.time()-start_time)  + " seconds"
    print "Tweets being processed "
    tweet_count= count_unicodeTweets(df)
    filterTweets(df)
    output_filepath = argv[2]
    to_file(df,output_filepath,tweet_count)
    print "Output file created at " + argv[2]
    print "Execution took " + str(time.time()-start_time) + " seconds"

if __name__ == "__main__":
    main(sys.argv)