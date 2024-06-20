[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=15232398&assignment_repo_type=AssignmentRepo)
# Please add your team members' names here. 

## Team members' names 

1. Student Name: Rohan Nayak

   Student UT EID: rsn474

2. Student Name: Ayaan Nazir

   Student UT EID: an29256

3. Student Name: Nikhil Kalidasu

   Student UT EID: nk23452

4. Student Name: 

   Student UT EID: 


## Task 1: Data Cleanup
In our project, we handled data cleanup by checking if the corresponding strings could be converted 
into Floats and DateTime objects, checking if trip_time_in_secs lasted the duration of pickup_datetime 
and dropoff_datetime (with a margin of error of 1 second), checking if longtitude and latitude values 
were non-zero (zero is in the middle of the ocean, somewhere a NYC cab cannot travel to), and checking 
if the payment type were CSH or CRD. We used "taxi-data-sorted-small.csv.bz2" to test data cleaning, and
found inconsistencies in payment type and coordinates. Here are some examples:

1. Negative balance error
DBA03C39ED8C744C7624D3E40E88893D,C069C625BEA9376E5A1D1BB7A58FBE90,2013-08-24 07:07:10,2013-08-24 07:12:00,290,0.77,-74.014198,40.715660,-74.010475,40.725368,CSH,-100.00,0.00,-0.50,0.00,0.00,-100.50
-150.5

2. Float error
CE423C54E5BC3FAED4A07B4F64742172,E8FD87E427302251F4DE131642BDA5FC,2013-01-05 06:20:51,2013-01-05
06:23:16,0,0.00,-73.789726,40.647003,,,CRD,63.00,0.00,0.00,0.00,0.00,
63.00

3. Invalid payment type
A954A71B6D44265AE756BF807E069396,D5CA7D478A14BA3BBFC20153C5C88B1A,2013-01-02 11:43:23,2013-01-02 11:51:28,484,1.40,-73.979027,40.754719,-73.996536,40.744518,DIS,7.50,0.00,0.50,0.00,0.00,8.00

4. Float error
37BDEA2E54A3B70CBFA5B0D1C3A75FE2,730A179A7F97126B694CABFB93CC3A0C,2013-01-03 12:27:06,2013-01-03
12:32:09,0,0.00,-73.995743,40.744186,,,CRD,5.50,0.00,0.00,0.00,0.00,5.50

5. Negative balance error
3DF0F9565A7E26AA877CDB83DD64F686,7091525186C94A0DEDFB4C6AE6C5ED08,2013-08-23 18:49:10,2013-08-23 18:49:00,-10,0.00,0.000000,0.000000,0.000000,0.000000,CSH,-100.00,0.00,0.00,0.00,0.00,-100.00
-100.5



## Task 2: Data Sorting
The sorting is done with the use of a PriorityQueue and a Map for effeciency purposes.
The elemenets are added to the Priority queue and they are automatically sorted based
on the comparator that is used. This is first done with the help of the Map. Elements
that are present in the PriorityQueue are then removed in descending order to provide
the sorted file.


##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 


# Project Template

This is a Java Maven Project Template


# How to compile the project

We use Apache Maven to compile and run this project. 

You need to install Apache Maven (https://maven.apache.org/)  on your system. 

Type on the command line: 

```bash
mvn clean compile
```

# How to create a binary runnable package 


```bash
mvn clean compile assembly:single
```


# How to run

```bash
mvn clean compile  exec:java -Dexec.args="WikipediaPagesOneDocPerLine.txt.bz2 300"
```



```bash
mvn clean compile  exec:java -Dexec.executable="edu.utexas.cs.cs378.Main"  -Dexec.args="WikipediaPagesOneDocPerLine.txt.bz2 500"
```


Input file is: 

WikipediaPagesOneDocPerLine.txt.bz2 

Data line batch size is: 500

You can modify the batch size based on available memory.


If you run this over SSH and it takes a lots of time you can run it in background using the following command

```bash
nohub mvn clean compile  exec:java -Dexec.executable="edu.utexas.cs.cs378.Main"  -Dexec.args="WikipediaPagesOneDocPerLine.txt.bz2 500"  & 
```

We recommend the above command for running the Main Java executable. 
