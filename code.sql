
#Print every department where the average salary per employee is lower than $500!

select a.department_name , average(b.salary ) as mean_salary

from employees as a join salaries as b 
on a.employee_id=b.employee_id 
having mean_salary<=500




select user_id, count(distinct( event_date_time)) as unique_events from event_log 
group by 1 
having unique_events between 1000 and 2000;


select a.author_name ,sum(b.sold_copies) as total_sold from authors as a join books as b on 
a.book_name=b.book_name 
group by 1
order by total_sold Desc limit 3; 

select a.* from alumni as a join evaluation as b on a.student_id=b.student_id where grade>=16;


WTIH HIGH_GRADE (
SELECT STUDENT_ID FROM EVALUATION WHERE GRADE>=16)

SELECT * FROM ALUMNI WHERE STUDENT_ID IN (SELECT STUDENT_ID FROM HIGH_GRADE)

select * from beverages where fruid_pct >=35 and fruid_pct<=40;


select * from beverages where contributed_by not like '% %';

select contributed_by, average(fruit_pct) as mean_f_p from beverages 
group by 1 order by 2; 


JAR does not exist or is not a normal file: /usr/lib/hadoop-3.3.0/share/hadoop/tools/lib/hadoop
-streaming-3.2.1.jar


hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -input input -output output
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar -file ~/mapper.py -mapper ~/mapper.py -file ~/reducer.py -reducer ~/reducer.py -numReduceTasks 3 -input input -output output

MySQL
SELECT first_name, last_name FROM employees;

SHOW TABLES;

DESCRIBE departments;

SELECT *
FROM employees;



SELECT
    employee_id AS Employee,
    employee_id AS "Employee Id",
    last_name Surname,
    first_name "First Name"
FROM employees;



SELECT DISTINCT department_id, manager_id
FROM employees;



SELECT employee_id, last_name, hire_date
FROM employees
ORDER BY last_name;



SELECT employee_id, last_name, first_name
FROM employees
ORDER BY last_name DESC, first_name;


#Sorting by unselected columns


SELECT employee_id, last_name, hire_date
FROM employees
ORDER BY manager_id, hire_date DESC;


#If you want NULL values to be at the bottom, you can first sort rows by IS NULL:


SELECT employee_id, last_name, manager_id, commission_pct
FROM employees
WHERE manager_id = 100
ORDER BY commission_pct IS NULL, commission_pct;


#Concatenating columns
SELECT CONCAT(first_name, last_name)
FROM employees;


SELECT CONCAT(first_name, "'s last name is ", '"', last_name, '"')
FROM employees;


###Performing calculations
SELECT
    last_name,
    salary,
    salary * 0.10 AS "Increase",
    salary + (salary * 0.10) AS "New Salary"
FROM employees;

#Working with NULLs
#Any number multiplied by NULL is NULL


SELECT 10 * NULL, NULL * (34 + 123);

#You can use IFNULL to replace NULL values with 0:

SELECT IFNULL(10, 0), IFNULL(NULL, 0);

SELECT 10 * IFNULL(NULL, 0);




###String functions

#To get the length of a string:

SELECT LENGTH('Hello');



#To change the case of a string:

SELECT UPPER('Hello'), LOWER('Hello');


#To find the first occurrence of a substring in a string:

SELECT INSTR('Hello', 'el');



#To find the substring at a given position in a string:

SELECT SUBSTR('Hello', 2, 3);


#To replace part of a string:

SELECT REPLACE('Hello', 'l', 'x');



#To trim white space from a string:

SELECT LTRIM(' Hello '), RTRIM(' Hello '), TRIM(' Hello ');

#To pad a string to a fixed length:

SELECT LPAD('Hello', 10, 'x'), RPAD('Hello', 10, 'x');



###Numeric functions
#To round a number to a given number of decimal places:

SELECT ROUND(3.247), ROUND(3.247, 1), ROUND(3.247, 2);


#To round a number up or down to the nearest integer:

SELECT CEILING(3.247), FLOOR(3.247);

#To truncate a number to a given number of decimal places:

SELECT TRUNCATE(3.247, 0), TRUNCATE(3.247, 1), TRUNCATE(3.247, 2);


#To find the absolute value of a number:

SELECT ABS(0), ABS(3.247), ABS(-3.247);


#To raise one number to the power of another:

SELECT POWER(2, 3), POWER(4, -1), POWER(16, 0.5);

#To find the remainder when one number is divided by another:

SELECT MOD(10, 3), 10 MOD 3, 10 % 3;


#Date functions
SELECT CURDATE();

#To get the number of days between two dates:

SELECT
    DATEDIFF('2020-06-10', '2020-06-10') AS 'A',
    DATEDIFF('2020-06-10', '2020-06-12') AS 'B',
    DATEDIFF('2020-06-10', '2020-06-08') AS 'C';


#To get the various parts of a date:

SELECT
    DAYNAME('1968-06-24') AS 'DayName',
    DAY('1968-06-24') AS 'Day',
    MONTH('1968-06-24') AS 'Month',
    MONTHNAME('1968-06-24') AS 'MonthName',
    YEAR('1968-06-24') AS 'Year';



#To format a date in a certain way:

SELECT DATE_FORMAT('1968-06-24', '%W, %D %M, %Y');


#The CASE function
SELECT
    last_name,
    Salary,
    CASE
        WHEN (salary >= 10000) THEN 'Level 5'
        WHEN (salary >= 8000) THEN 'Level 4'
        WHEN (salary >= 5000) THEN 'Level 3'
        WHEN (salary >= 2500) THEN 'Level 2'
        ELSE 'Level 1'
    END AS Level
FROM employees
ORDER BY salary DESC;


#To find employees whose id starts with 1, has any number in the middle, and ends with 9, and who was hired in the year 2008, you could use:


SELECT employee_id, last_name, first_name, hire_date
FROM employees
WHERE employee_id LIKE '1_9' AND hire_date LIKE '2008-%';


#To find employees whose last name starts with 'A' or 'B' you could use:

SELECT employee_id, last_name, first_name
FROM employees
WHERE last_name LIKE 'A%' OR last_name LIKE 'B%';









SELECT AVG(salary), SUM(salary)/COUNT(salary)
FROM employees;

#GROUP_CONCAT
#Rather than counting or summing the values you can combine them into a string, using the GROUP_CONCAT function:
SELECT GROUP_CONCAT(city)
FROM locations


SELECT GROUP_CONCAT(city SEPARATOR '; ')
FROM locations


SELECT GROUP_CONCAT(city ORDER BY city DESC SEPARATOR '; ')
FROM locations



SELECT GROUP_CONCAT(city SEPARATOR '; ' ORDER BY city DESC)
FROM locations


-- Leave NULL values of commission_pct as NULL
SELECT
    COUNT(*) AS total_rows,
    COUNT(salary * commission_pct) AS included_rows,
    AVG(salary * commission_pct) AS average
FROM employees



-- Change NULL values of commission_pct into 0
SELECT
    COUNT(*) AS total_rows,
    COUNT(salary * IFNULL(commission_pct, 0)) AS included_rows,
    AVG(salary * IFNULL(commission_pct, 0)) AS average
FROM employees




SELECT department_id, COUNT(*) AS total
FROM employees
GROUP BY department_id
HAVING total >= 10
ORDER BY total DESC;


select DISTINCT a.Title, b.PaidEach- a.Cost AS Profit from Books as a join OrderItems as b on a.ISBN =b.ISBN
order by Profit DESC, Title; 






select DISTINCT Title, Retail- Cost AS Profit from Books 
order by Profit DESC, Title; 


Produce a list of books, showing their title and the number of orders, sorted by number of orders descending, then name ascending.


select a.Title, count(distinct b.OrderId) as Number from Books as a join OrderItems as b on a.ISBN =b.ISBN  
order by Number DESC, Title; 



SELECT a.OrderDate, sum(b.Quantity) as Number, sum(b.Cost) as Retail
from Orders as a join OrderItems as b on a.OrderId=b.OrderId join Books as c on c.ISBN =b.ISBN  
group by 1
order by 1;





select b.OrderId, b.PersonId, b.OrderDate,b.ShipDate, b.ShipCity, b.ShipState,b.ShipPostcode
where ShipDate is NULL
order by OrderDate


Select a.FirstName, a.LastName,
sum(c.Quantity) as NumBooks  ,
sum(c.PaidEach) as AmountSpent, sum(d.Cost) as AmountSpent2,sum(d.Retail) as AmountSpent3,sum(c.Quantity*c.PaidEach) as AmountSpent4
from People as a join 
Orders as b  on a.PersonId=b.PersonId join
OrderItems as c on c.OrderId=b.OrderId join
Books as d on c.ISBN=b.ISBN 
group by a.FirstName, a.LastName
order by 4 desc;


SELECT a.Title
FROM Books
WHERE ISBN NOT IN (
    SELECT ISBN 
    FROM OrderItems 
);


select AVG(ShipDate-OrderDate)
from Orders
where ShipDate is not NULL



select a.Title, count(distinct(b.PersonId)) as NumAuthors, GROUP_CONCAT(c.LastName  SEPARATOR ', ' ORDER BY c.LastName ASC) 
from Books as a join
BookAuthors as b on a.ISBN=b.ISBN join 
People as c on b.PersonId=c.PersonId
group by Title 







select Titlem, NumAuthors, Authors from (
a.Title, count(distinct(b.PersonId)) as NumAuthors,
GROUP_CONCAT(c.LastName  SEPARATOR ', ')  as Authors
from Books as a join
BookAuthors as b on a.ISBN=b.ISBN join 
People as c on b.PersonId=c.PersonId ) as tmp
where  NumAuthors>1
group by Title 
order by Title





select a.LastName, a.FirstName a.OrderDate, a.maxi  from (
select  b.LastName, b.FirstName,c.OrderDate,  max(c.OrderDate) as maxi
from People as b join Orders as c 
) a

order by 1,2







from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        for word in value.strip().split():
            yield word, 1
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Job.run()






from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, line):
        numwords = len(line.split())
        yield "words", numwords


    def reducer(self, key, values):
        i,totalL,totalW=0,0,0
        for i in values:
            totalL += 1
            totalW += i     
        yield "avg", totalW/float(totalL)

if __name__ == '__main__':
    Job.run()







from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)
if __name__ == '__main__':
    Job.run()


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    MRWordFreqCount.run()








CREATE TABLE ourText (
    line STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS textfile;

LOAD DATA LOCAL INPATH 'text.txt' OVERWRITE INTO TABLE ourText;

SELECT word, COUNT(*)
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, ' ')) words as word
GROUP BY word;



create table sentences as
SELECT sentence
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, '.')) sentences as sentence;





How many lines are in the file?

SELECT COUNT(*) FROM ourText;
What is the length of each line, from shortest to longest?

SELECT LENGTH(line) FROM ourText ORDER BY LENGTH(line);
What is the average length of the lines (in characters)?

SELECT AVG(LENGTH(line)) FROM ourText;
How many words are in each line of the file?

We first need to break the line into words - we can do that using Hive's split() function, splitting the line at its spaces. Then we can use Hive's size() function to get the number of resulting words:

SELECT SIZE(SPLIT(line, ' ')) FROM ourText;
How many words are in the file?

We can get this by summing the number of words in each line:

SELECT SUM(SIZE(SPLIT(line, ' '))) FROM ourText;
What is the average word length?

To answer this we need the total length of the words, and the number of words, and then divide the former by the latter. We can get the number of words using the previous query. What about the total length of words? We can get the total length of words in a line by removing its spaces (replace them with nothing) and then counting the number of characters that remain, then we can sum these to get the total length of words. So we can use the following query:

SELECT SUM(LENGTH(REPLACE(line, ' ', '')))/SUM(SIZE(SPLIT(line, ' '))) FROM ourText;
Explode
Hive has a function EXPLODE() which turns an array of values into rows. Try the following query:

SELECT EXPLODE(SPLIT(line, ' ')) AS word FROM ourText;
In effect, this query generates a table of words that we can query to get answers about words. This involves using the above query as a subquery in other queries. For example:

How many words are there?

SELECT COUNT(*)
FROM (SELECT EXPLODE(SPLIT(line, ' ')) AS word FROM ourText) AS words;
What is the average word length?

SELECT AVG(LENGTH(word))
FROM (SELECT EXPLODE(SPLIT(line, ' ')) AS word FROM ourText) AS words;
What is the frequency of each word?

SELECT word, COUNT(*)
FROM (SELECT EXPLODE(SPLIT(line, ' ')) AS word FROM ourText) AS words
GROUP BY word;
Lateral View
Hive has a way of doing this more efficiently, using LATERAL VIEW. This gives a way of merging the results of EXPLODE back together with the original table.

SELECT word
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, ' ')) words as word;
What are the unique words?

SELECT DISTINCT word
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, ' ')) words as word;
What is the average word length?

SELECT AVG(LENGTH(word))
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, ' ')) words as word;
What is the frequency of each word?

SELECT word, COUNT(*)
FROM ourText LATERAL VIEW EXPLODE(SPLIT(line, ' ')) words as word
GROUP BY word;





SELECT sentences, COUNT(*)
FROM (SELECT EXPLODE(SPLIT(line, ' ')) AS sentence FROM ourText) AS sentences
group by sentences;

#hive -f t.hql


CREATE TABLE IF NOT EXISTS text2(
    line VARCHAR(50) NOT NULL
);


INSERT INTO text2
VALUES('this is a sentence.'),
('and this is another one.'),
('Last sentence is here.');



CREATE TABLE IF NOT EXISTS text2(
    line VARCHAR(50) NOT NULL
);


INSERT INTO text2
VALUES('this is a sentence.'),
('and this is another one.'),
('Last sentence is here.');


week 5
CREATE TABLE ourText (
    line STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\n'
STORED AS textfile;


LOAD DATA LOCAL INPATH 'walden.txt' OVERWRITE INTO TABLE ourText;

drop table if exists one; 
create table one as select 
a.firstletter,
ROUND(SUM(LENGTH(REPLACE(a.line, ' ', '')))/SUM(SIZE(SPLIT(a.line, ' '))) ,2)
from (select  concat("Letter ",   upper(substr(line,1,1)) ,":"  ) as firstletter, line from ourText ) a
where a.firstletter !="Letter :"
group by a.firstletter
;


select * from one;




from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        for word in value.strip().split('.'):
	        #yield "chars", len(value)
	        #yield "words", len(value.split())
	        #yield "lines", 1
	        yield "Letter "+word[0:1],len(value)/len(value.split())

   
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Job.run()

 
from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        for word in value.strip().split('.'):
            if word[0:1] !="" and word[0:1] !=" ":
	            yield "Letter "+word[0:1] +":",len(value)/len(value.split())

   
    def reducer(self, key, values):
        yield key, mean(values)
if __name__ == '__main__':
    Job.run()
    for v in values:
        Sum += v
        n += 1
    return Sum / n







lines = sc.textFile("text.txt")


graphDF
  // I want to know the number of edges to the nodes of the same type
  .where($"type_from" === $"type_to" && $"from" =!= $"to")
  // I only want to count the edges outgoing from a node,
  .groupBy($"from" as "nodeId", $"type_from" as "type")
  .agg(count("*") as "numLinks")


from pyspark import SparkContext, SparkConf
sc = SparkContext('local', 'rdd')
text = sc.textFile("text.txt")
frequencies.saveAsTextFile("results")
sc.stop()

 
frequencies.saveAsTextFile("results")



export PATH
export JAVA_HOME=$(/usr/libexec/java_home)
export SPARK_HOME=~/spark-2.3.0-bin-hadoop2.7
export PATH=$SPARK_HOME/bin:$PATH
export PYSPARK_PYTHON=python3
export PATH="/usr/local/opt/openjdk@11/bin:$PATH"


/usr/local/Cellar/apache-spark/3.1.1/bin/spark-submit



Part A
--------------------
regular.py
--------------------
import sys
import re
results = []
for line in sys.stdin:
    results.extend(line.split())

max_words=max(len(re.sub('[^a-zA-Z0-9]', ' ', w)) for w in results)
print("The longest word has ", max_words, " characters.")

--------------------
mapper.py
--------------------
#!/usr/bin/python
#mapper
#!/usr/bin/python
import sys
for line in sys.stdin:
        for word in line.strip().split():
            print (word)

--------------------
reducer.py
--------------------
#!/usr/bin/python
import sys
import re
data=[]
for line in sys.stdin:
    data.append(line.strip())

max_words=max(len(re.sub('[^a-zA-Z0-9]', ' ', w)) for w in data)

print("The longest word has ", max_words, " characters.")


Part B
--------------------
regular.py
--------------------
import sys
import pandas as pd

employees = pd.read_csv(sys.stdin)
employees.columns=['employee_id',
                   'first_name',
                   'last_name',
                   'email',
                   'phone_number',
                   'hire_date',
                   'salary'
                   ]
   
     
   
avg_pay=round(employees["salary"].mean())

print("The average salary is $", avg_pay, ".",sep='')


--------------------
mapper.py ​ --- For this mapper file, we should also add #!/usr/bin/python at the top of the file.
--------------------
#!/usr/bin/python
import sys
for line in sys.stdin:
    fields = line.strip().split(',')
    print(fields[-1])
    

--------------------
reducer.py
--------------------
#!/usr/bin/python
import sys
max_salary = 0
salary=[]
   
for line in sys.stdin:
    salary.append(line.strip())

salary = list(map(int, salary))
avg_pay=round(sum(salary)/len(salary))

print("The average salary is ", avg_pay, ".",sep='')




week 4


import sys
results = {}
for line in sys.stdin:
	for word in line.strip().split():
    #word, frequency = line.strip().split('\t', 1)
    	results[word] = results.get(word, 0) + int(frequency)
words = list(results.keys())
words.sort()
max_words=max(len(re.sub('[^a-zA-Z0-9]', ' ', w)) for w in words)
print("The longest word has ", max_words, " characters.")

for word in words:
    print(word, results[word])
 


#!/usr/bin/python
import sys
for line in sys.stdin:
        for word in line.strip().split():
            print (word)




import sys
import re
data=[]
for line in sys.stdin:
    data.append(line.strip())

max_words=max(len(re.sub('[^a-zA-Z0-9]', ' ', w)) for w in data)

print("The longest word has ", max_words, " characters.")




import sys
max_salary = 0
for line in sys.stdin:
    index, value = line.split('\t')
    max_salary = max(max_salary, int(value))
print('The maximum salary is $', max_salary)











part b original

import sys
salary=[]
   
for line in sys.stdin:
    salary.append(line.strip())

salary = list(map(int, salary))
avg_pay=round(sum(salary)/len(salary))

print("The average salary is $ ", avg_pay, ".",sep='')



part b new failing maps
import sys
salary=[]
for line in sys.stdin:
    index, value = line.split('\t')
    salary.append(value)

salary = list(map(int, salary))
avg_pay=round(sum(salary)/len(salary))

print("The average salary is $ ", avg_pay, ".",sep=''



part b new doesnt work

#!/usr/bin/python

import sys

results = {}
for line in sys.stdin:
    for s in line.strip().split():
        s, frequency = line.strip().split('\t', 1)
        results[s] = results.get(s, 0) 
salary = list(results.keys())
salary = list(map(int, salary))

avg_pay=round(sum(salary)/len(salary))
#print(int(salary[1]))
print(len(salary))
print(sum(salary))
print("The average salary is $ ", avg_pay, ".",sep='')



#!/usr/bin/python
import sys
import re
results = {}
for line in sys.stdin:
    for word in line.strip().split():
        word, key = line.strip().split('\t')
        results[word] = results.get(word, 0) + int(frequency)
words = list(results.keys())
words.sort()
max_words=max(len(re.sub('[^a-zA-Z0-9]', ' ', w)) for w in words)
print("The longest word has ", max_words, " characters.")




import sys

current_key = None
word_sum = 0
for line in sys.stdin:
	key, count = line.strip().split('\t', 1)
    count = int(count)
    if current_key != key:
        if current_key:
            print "%s\t%d" % (current_key, word_sum)
        word_sum = 0
        current_key = key
    word_sum += count

if current_key:
    print "%s\t%d" % (current_key, word_sum)






week 6 
 
#First, create an RDD:

text = sc.textFile("text.txt")
#You now have an RDD called "text" which contains the contents of the file "text.txt". To see the number of items in the RDD you can use the count() action: 

text.count()
#To see the first item of the RDD you can use the first() action:

text.first()
#To see the first three items of the RDD you can use the take() action:

text.take(3)
#To see the contents of the RDD you can use the collect() action:

text.collect()
#To see the lines that contain "war" you first filter the RDD, then collect it:

text.filter(lambda line: "war" in line).collect()
#The argument of filter() is an anonymous function, which takes a line of the file as input and returns true if this line contains “war” and false otherwise. As a result, only lines containing “war” are included in the resulting RDD.

#You can use the map() function to map each line of the file to the number of words the line contains. Let's save it as a new RDD called "wordNums":

wordNums = text.map(lambda line: len(line.split()))
#The argument of map() is an anonymous function, which takes a line as the input and returns the number of words in the line (we're assuming that words are separated by a space). You can view the items in wordNums using the collect() action:

wordNums.collect()
#To the largest number of words in a line we can apply the reduce() action to wordNums:

wordNums.reduce(lambda result, value: max(result, value))
#You can try to cache the data in memory:

wordNums.cache()

#Word frequency
#Now let's use Spark to get the frequency of words in the file.

#First, let's create an RDD that contains the words in the file. We can do this by applying the flatMap() transformation to text, as follows:

words = text.flatMap(lambda line: line.split()) 
#We are using flatMap() here, rather than map(), because line.split() returns a list of words for each line, and we want to "flatten" all of those lists of words into a single list of words.

#We can get the total number of words using count():

words.count()
#We can get the number of distinct words by first transforming words using distinct():

words.distinct().count()
#Compare the results with words.count(). You should see that duplicate words have not been counted twice.

#Now count the frequency of each word.

#First, use map() to transform words into a new RDD that contains each of the words in words paired with a 1 (i.e <word, 1>):

pairs = words.map(lambda word: (word, 1))
#Next, transform this RDD into a new RDD that contains the word frequencies:

frequencies = pairs.reduceByKey(lambda total, value: total + value)
#Finally, show the results:

frequencies.collect()
#You should get the same results as we did using MapReduce, MRJob and Hive.

#You could also combine these commands into a single command:

text.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda total, value: total + value).collect()
frequencies.saveAsTextFile("results")






#!/usr/bin/python
import sys

max_wordlen = 0
for line in sys.stdin:
    for wordlen in line.strip().split():
        wordlen, key = line.strip().split('\t')
        max_wordlen = max(max_wordlen, int(wordlen))
print("The longest word has ", max_wordlen, " characters.")








sex_age = {}

#Partitoner
for line in sys.stdin:
    line = line.strip()
    sex, age = line.split('\t')

    if sex in sex_age:
        sex_age[sex].append(int(age))
    else:
        sex_age[sex] = []
        sex_age[sex].append(int(age))

#Reducer
for sex in sex_age.keys():
    ave_age = sum(sex_age[sex])*1.0 / len(sex_age[sex])
    print '%s\t%s'% (sex, ave_age)



import sys
#salary=[]
salary = {}
for line in sys.stdin:
    value,index  = line.split('\t')
    if index in salary:
        salary[index].append(int(value))
    else:
        salary[index]= []
        salary[index].append(int(value))
 
#salary = list(map(int, salary))


for value in salary.keys():
    ave_pay = sum(salary[value])*1.0 / len(salary[value])
    print("The average salary is $ ", avg_pay, ".",sep='')


avg_pay=round(sum(salary)/len(salary))

print("The average salary is $ ", avg_pay, ".",sep='')





#!/usr/bin/python

import sys
#salary=[]
salary=0
num_salary=0
for line in sys.stdin:
    value,index  = line.split('\t')
    salary=salary+int(value)
    num_salary=num_salary+1
avg_pay=round(salary/num_salary)
#salary = list(map(int, salary))
#avg_pay=round(sum(salary)/len(salary))

print("The average salary is $ ", avg_pay, ".",sep='')

#week 5 

If there are any to whom it is no interruption to acquire these things, and who know how to use them when ac
quired, I relinquish to them the pursuit. 


from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        for word in value.strip().split('.'):
            word=word.strip()
            if word[0:1] !="" and word[0:1] !=" ":
                #yield value ,len(value.strip(" "))
                #print(len(value))
	            #yield "Letter "+word[0:1] +":",len(value)/len(value.split())
                yield word,  len(value)
   
    def reducer(self, key, values):
        yield "Letter "+key[0:1] +":", sum(values)/len(key.split())
        #yield key, min(values)
if __name__ == '__main__':
    Job.run()



drop table if exists one; 
create table one as select 
a.firstletter,
ROUND(SUM(LENGTH(REPLACE(REPLACE(a.line, ' ', ''), '-', 'z')))/SUM(SIZE(SPLIT(TRIM(a.line), ' '))),2) 
from (select  concat("Letter ",   upper(substr(line,1,1)) ,":"  ) as firstletter, line from ourText ) a
group by a.firstletter
;



    def mapper(self, key, value):
        yield "Letter "+value[0:1] +":",len(value)
        yield "No. lines", 1
        yield "No. words", len(value.split())
        yield "No. characters", len(value)








from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        yield "Letter "+value[0:1] +":",(len(value) - value.count(' '))/len(value.split())
        yield "No. lines", 1
        yield "No. words", len(value.split())
        yield "No. characters", (len(value) - value.count(' '))
    def combiner(self, key, value):
      avg, count = 0, 0
      for tmp, c in value
        avg = (avg * count + tmp * c) / (count + c)
        count += c
      return (key, (avg, count))
    def reducer(self, key, value):
      key, (avg, count) = self._reducer_combiner(key, value)
      yield (key, avg)
if __name__ == '__main__':
    Job.run()








from mrjob.job import MRJob
class Job(MRJob):
    def mapper(self, key, value):
        yield "Letter "+value[0:1] +":",(len(value) - value.count(' '))/len(value.split())
        yield "No. lines", 1
        yield "No. words", len(value.split())
        yield "No. characters", (len(value) - value.count(' '))
    def combiner(self, key, values):
        yield key, sum(values)
    def reducer(self, key, values):
        yield key, sum(values)
if __name__ == '__main__':
    Job.run()











