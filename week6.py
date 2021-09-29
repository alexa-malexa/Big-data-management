#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 09:57:46 2021

@author: alexakelly
"""

#week 6 
 
#First, create an data:

import findspark
findspark.init() 
    
from pyspark import SparkContext
from pyspark.sql import SparkSession
 

sc = SparkContext("local", "First App")    
spark = SparkSession(sc)

graph = sc.textFile("/Users/alexakelly/Desktop/Alexa documents/Big data management/graph.txt")
 
data = graph.map(lambda k: k.split(" "))
data.collect()
data2=data.map(lambda x: (x[1], x[3]))
data2.collect()
data2=data2.map(lambda x: (float(x[0]), float(x[1])))
data2.reduceByKey(lambda x,y: x+y).collect()
storage = (0,0)
data3 = data2.aggregateByKey(storage, lambda x,y: (x[0]+y,x[1]+1),lambda x,y:(x[0]+y[0],x[1]+y[1]))
data3 = data3.mapValues(lambda x: round(x[0]/x[1],2) ).collect()
result = sc.parallelize(data3)
result=result.sortBy(lambda x: (-x[1]))
result.collect()




def avg_map_func(row):
    return (row[0], (row[2], 1))

def avg_reduce_func(value1, value2):
    return ((value1[0] + value2[0], value1[1] + value2[1])) 

dataset_data.map(avg_map_func).reduceByKey(avg_reduce_func).mapValues(lambda x: x[0]/x[1]).collect()




import numpy as np

a = np.array(graph.collect())
print(a)



#You now have an data called "graph" which contains the contents of the file "graph.txt". To see the number of items in the data you can use the count() action: 

graph.count()
#To see the first item of the data you can use the first() action:

graph.first()
#To see the first three items of the data you can use the take() action:

graph.take(3)
#To see the contents of the data you can use the collect() action:

graph.collect()
#To see the lines that contain "war" you first filter the data, then collect it:

graph.filter(lambda line: "war" in line).collect()
#The argument of filter() is an anonymous function, which takes a line of the file as input and returns true if this line contains “war” and false otherwise. As a result, only lines containing “war” are included in the resulting data.

#You can use the map() function to map each line of the file to the number of words the line contains. Let's save it as a new data called "wordNums":

wordNums = graph.map(lambda line: line.split(' '))
#The argument of map() is an anonymous function, which takes a line as the input and returns the number of words in the line (we're assuming that words are separated by a space). You can view the items in wordNums using the collect() action:

wordNums.collect()
#To the largest number of words in a line we can apply the reduce() action to wordNums:
wordNums.first()


wordNums.reduce(lambda result, value: max(result, value))
#You can try to cache the data in memory:

wordNums.cache()

#Word frequency
#Now let's use Spark to get the frequency of words in the file.

#First, let's create an data that contains the words in the file. We can do this by applying the flatMap() transformation to graph, as follows:

words = graph.flatMap(lambda line: line.split(' ')) 
#We are using flatMap() here, rather than map(), because line.split() returns a list of words for each line, and we want to "flatten" all of those lists of words into a single list of words.

words.collect()
words.first()




#We can get the total number of words using count():

words.count()
#We can get the number of distinct words by first transforming words using distinct():

words.distinct().count()
#Compare the results with words.count(). You should see that duplicate words have not been counted twice.

#Now count the frequency of each word.

#First, use map() to transform words into a new data that contains each of the words in words paired with a 1 (i.e <word, 1>):

pairs = words.map(lambda word: (word, 1))
#Next, transform this data into a new data that contains the word frequencies:
pairs.collect()
    


frequencies = pairs.reduceByKey(lambda total, value: total + value)
#Finally, show the results:

frequencies.collect()
#You should get the same results as we did using MapReduce, MRJob and Hive.

#You could also combine these commands into a single command:

graph.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda total, value: total + value).collect()
frequencies.saveAsgraphFile("results")







130+200+300+220+140+350+150+124
 
from pyspark.sql.functions import col, split

df = spark.read.option("header", "false").csv("/Users/alexakelly/Desktop/Alexa documents/Big data management/graph.txt")
df.createOrReplaceTempView("df")
df2=df.withColumn('temp', split('_c0', ' ')).select(*(col('temp').getItem(i).alias(f'c{i}') for i in range(4)))
df2.show()  
df2.createOrReplaceTempView("df2")           
results=spark.sql("SELECT col1, round(sum(col3)/count(col3),2) as average FROM df2 group by col1 order by 2 desc")                              

results.show() 
results.write.format("csv").save('output')
      
 
results.rdd.map().saveAsTextFile("result-sql")
results.write.format("text").option("header", "false").mode("append").save("output.txt")

results.rdd.map(lambda x : str(x[0]) + "," + x[1]).saveAsTextFile('save_text')
results.coalesce(1).write.format('csv').save('save_text2')

df.coalesce(1).write.option("inferSchema","true").csv("/newFolder",header = 
'true',dateFormat = "yyyy-MM-dd HH:mm:ss")






from pyspark.sql.functions import  udf

orders =sc.textFile("/Users/alexakelly/Desktop/Alexa documents/Big data management/orders.csv") \
    .map(lambda line: line.split(",")).filter(lambda line: len(line)>1).map(lambda line: (line[0],line[5]))  
header = orders.first()
orders=orders.filter(lambda x:x != header)
to_a_date =  udf (lambda x: datetime.strptime(x, '%Y/%m/%Y\d'), DateType())

orders=orders.map(lambda x: (to_a_date(x[0]), int(x[1])))
orders.collect()


result =orders.reduceByKey(lambda x,y: round(x+y,0))
result=result.sortBy(lambda x: (-x[1]))
result.collect()   


 
 

#week 6 q2 part b 

from pyspark.sql.functions import col, split
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("sql").getOrCreate()

orders = spark.read.format("csv").option("header", "true").load("/Users/alexakelly/Desktop/Alexa documents/Big data management/orders.csv")
orders.createOrReplaceTempView("orders")
orders.show()
         
results=spark.sql("SELECT OrderDate, cast(sum(Quantity) as int) as day_tot FROM orders group by 1 order by 2 desc")                              
results.show()
results.coalesce(1).write.format('csv').save('result-sql')

spark.stop()


#week 5 

from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):


   
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Job.run()




my_file = open("walden.txt", "r")


with open('walden.txt', 'r') as file:
    data = file.read().replace('\n', '')

    
 
for sentence in data.strip().split('.'):
    sentence=sentence.strip()
    print("Letter "+sentence[0:1] +":",(len(sentence) - sentence.count(' '))/len(sentence.split()))
    
    
    print( "Letter "+sentence[0:1] +":",len(sentence)/len(sentence.split()))
    "".join(sentence.split())
    print( "Letter "+sentence[0:1] +":",len(sentence)/len(sentence.split()))
    if sentence[0:1] !="" and sentence[0:1] !=" ":
        print(len(sentence), "  ",len(sentence.split()) )
        print( "Letter "+sentence[0:1] +":",len(sentence)/len(sentence.split()))
        
        
        150/29
    3.9 +   4.230769230769231
     
sentence_1="As I did not teach for the good of my fellow-men, but simply for a livelihood, this was a failure."
sentence_2="As I preferred some things to others, and especially valued my freedom, as I could fare hard and yet succeed well, I did not wish to spend my time in earning rich carpets or other fine furniture, or delicate cookery, or a house in the Grecian or the Gothic style just yet."
       


sentence_1="Aaa bbb cc." 
sentence_2="Ab b. "     
        300/72

sent1_len= len(sentence_1.strip()) - sentence_1.count(' ')- sentence_1.count('.')
sent1_words=len(sentence_1.split())       
        
avg_words1= sent1_len/sent1_words
        

sent2_len= len(sentence_2.strip()) - sentence_2.count(' ') 
sent2_words=len(sentence_2.split())       
        
avg_words2= sent2_len/sent2_words
        
avg_word_length_total= (avg_words1+avg_words2)/2
        
avg_word_length_total2= (sent1_len + sent2_len)/(sent1_words+sent2_words)
              
        
13/5
        
 (3*2+2*2+1*1)/5 =2.2
        
        
from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        for word in value.strip().split('.'):
            if word[0:1] !="" and word[0:1] !=" ":
	            yield "Letter "+word[0:1] +":",len(value)/len(value.split())

   
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Job.run()

        
        
        
