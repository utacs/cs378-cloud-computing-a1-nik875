[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-718a45dd9cf7e7f842a935f5ebbe5719a5e09af4491e668f4dbf3b35d5cca122.svg)](https://classroom.github.com/online_ide?assignment_repo_id=15232398&assignment_repo_type=AssignmentRepo)
# Please add your team members' names here. 

## Team members' names 

1. Student Name: Rohan Nayak

   Student UT EID: rsn474

2. Student Name: Ayaan Nazir

   Student UT EID: an29256

3. Student Name: Nikhil Kalidasu

   Student UT EID: nk23452

4. Student Name: Satwik Misra

   Student UT EID: 


## Task 1: Data Cleanup
In our project, we handled data cleanup by checking if the corresponding strings could be converted 
into Floats and DateTime objects, checking if total_amount there are negative balances, and checking 
if the payment type were CSH or CRD. We used "taxi-data-sorted-small.csv.bz2" to test data cleaning, 
and found inconsistencies in payment types, totals, and data types. Here are some examples:

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
the sorted file. This sorting algorithm was used in parallel as we decompressed each 
batch of the bz2 file. These batches were then merged together in an external n-way
merge by selecting the highest values from each batch. The results can be seen by
running the algorithm on the small dataset.

## Task 3: Cloud Results
The following is the head after merging the large dataset:

1. 89EDAF45090C74611B52AFFC3E10A69D,664927CDE376A32789BA48BF55DFB7E3,2013-08-14 21:29:00,2013-08-14 21:53:00,1440,11.62,-73.993378,40.764465,-73.877792,40.826706,CSH,33.00,0.50,0.50,0.00,0.00,685908.10
2. CC664699259C9867E735976611A82F64,E4F99C9ABE9861F18BCD38BC63D007A9,2013-08-05 18:01:00,2013-08-05 18:10:00,540,1.80,-73.968628,40.762646,-73.979111,40.777706,CSH,8.50,1.00,0.50,0.00,0.00,541432.56
3. D4CA68ECC21536DE406F3D58C7813241,BE047851D97506885B99BDDFA7A13360,2013-08-17 00:59:19,2013-08-17 01:06:02,402,522133.00,-74.001732,40.719528,-73.993713,40.720119,CSH,82162.30,79.67,0.00,0.00,0.00,82241.97
4. E523F84E85708BCEB9FEB6F7825C0E08,F9A6ED413D476F4560D90BA51151DAFB,2013-08-04 13:10:35,2013-08-04 13:16:50,375,0.90,-74.014725,40.701820,-74.015640,40.709232,CSH,9000.60,0.05,0.00,0.00,0.00,9000.65
5. E2DF7E5E63A654B9A655FBDCC0B029BF,A92262E4AA9A8F8784A592E7ABC6E04F,2013-08-24 07:49:51,2013-08-24 08:13:44,1433,7.70,-73.949509,40.780476,-73.905334,40.867435,CSH,7025.50,577.92,80.05,0.00,0.00,7683.47
6. E2DF7E5E63A654B9A655FBDCC0B029BF,A92262E4AA9A8F8784A592E7ABC6E04F,2013-08-24 13:18:11,2013-08-24 13:33:06,895,2.70,-74.006096,40.739773,-73.975838,40.759300,CSH,7001.20,577.92,80.05,0.00,0.00,7659.17
7. E2DF7E5E63A654B9A655FBDCC0B029BF,9707EA6FF9C16FA88F2EB9D5EBECBC80,2013-08-05 10:41:06,2013-08-05 11:14:01,1974,10.50,-73.981850,40.763042,-73.861801,40.768368,CSH,5003.50,577.92,80.05,0.00,0.00,5661.47
8. 1560E7C1453E7996A61C96B7A8824EFE,7B19DE6D4D54999531BEB27F758F71F6,2013-08-09 14:05:00,2013-08-10 10:59:00,75240,0.00,0.000000,0.000000,0.000000,0.000000,CSH,2069.50,0.00,0.50,0.00,0.00,2070.00
9. E77A964307CF49B32AD77E298A4951D0,CFCD208495D565EF66E7DFF9F98764DA,2013-08-27 17:10:00,2013-08-27 17:12:00,120,0.00,0.000000,0.000000,0.000000,0.000000,CRD,999.00,854.50,0.00,0.00,0.00,1853.50
10. E77A964307CF49B32AD77E298A4951D0,CFCD208495D565EF66E7DFF9F98764DA,2013-08-13 16:35:00,2013-08-13 16:36:00,60,0.00,0.000000,0.000000,0.000000,0.000000,CRD,999.00,811.00,0.00,0.00,0.00,1810.00

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
