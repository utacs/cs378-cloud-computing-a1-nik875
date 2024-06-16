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

   1. Line 2: Invalid Coordinates
      All coordinates read 0.
   
   2. Line 14: Invalid Coordinates
      Dropoff longtitude and latitude read 0.
   
   3. Line 74: Invalid payment type
      Payment is listed as "UNK".

   4. Line 374: Invalid Coordinates
      All coordinates read 0.

   5. Line 516: Invalid payment type
      Payment is listed as "UNK".


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







