# Wuzzuf Jobs Web Analysis

Reading, cleaning wuzzuf(Egyptain jobs website) data set and produce statistics about job market in Egypt.\
dataset source : <a>https://www.kaggle.com/omarhanyy/wuzzuf-jobs</a> \
Project is done using java, displaying results as json on a spring boot web serivce. 
* This project is a final project for java module at AI track-ITI Egypt. 



# Running the project:

* Run the main and wait for console to print "Done!"
* Make sure you run it on 1.8 JDK.

# Overview



## DAO Constructor
 * Takes the dataset file path as argument 
 * Reads the data as RDD : Assigns a string RDD holding the dataset in the csv file to the class attribute "jobs"
 * Cleans the data : Removes entries with null values and the header
 * Splits RDD to separate Table columns and trim spaces
 * Mines the data in the columns.
 * Merge columns in one tablesaw Table and assign it's value to the class attribute "DataSetTable"


## Data reading and cleaning:

* For data reading we used spark rdd.
* For cleaning, a mix between rdd and tablesaw. 

### filterTable function:

* Function takes column name and size as input.
* Work on the sent column { counting by value , sorting }
* Returns A table of two columns, the first column is the Category and the second is the count, reduced to the sent size.
* Ex: size = 10 >> returns only 10 first rows.

### filterByTitle function:

* Function takes size as input.
* Calls filterTable() to return a table that holds "Title" versus count and print it in the console.
* Visualize the table using bar chart by calling the function makeBarChart()
* Extract table columns as lists and create a hash map using MakeMapFromLists() function
* returns the map


### filterByCompany function:

* Function takes size as input.
* Calls filterTable() to return a table that holds "Company" versus count and print it in the console.
* Visualize the table using pie chart by calling the function makePieChart()
* Extract table columns as lists and create a hash map using MakeMapFromLists() function
* returns the map


### filterByArea function:

* Function takes size as input.
* Calls filterTable() to return a table that holds Location "Areas" versus count and print it in the console.
* Visualize the table using bar chart by calling the function makeBarChart()
* Extract table columns as lists and create a hash map using MakeMapFromLists() function
* returns the map


### filterByExperience function:

* Function takes size as input.
* Extracts n from YearsExp column written as "n Yrs of Exp" .
* Converts n List into integer column "Numeric YearsExp" .
* Appends "Numeric YearsExp" column to the table .
* Calls filterTable() to return a table that holds "Numeric YearsExp" versus count and print it in the console.
* Extract table columns as lists and create a hash map using MakeMapFromLists() function
* returns the map

### filterBySkills function:

* Function takes size as input.
* Collects all skills in one list.
* Forms a string column "skills" from the list .
* Create a table with two columns ("skills" and "Count")
* Extract table columns as lists and create a hash map using MakeMapFromLists() function
* returns the map

### makeBarChart & makePieChart functions:

* Function takes 2 lists, size, the first list is string and the second is Integer.
* size specifies the number of wanted entries.
* Ex: size = 10 >> draw only 10 entires.
* Make the charts and saves it as Png in the /out folder.



## Web Service:

Using spring boot Application to create a server:
* The server runs on port 3030. If you want to change it, change the application.properties file.
* Using Rest controller. 
* All functions are Get requests. (You can find them in Analyzer web service Class)

### Popular Companies function:

access : <a>http://localhost:3030/PopularCompanies</a>
* Returns a ResponseEntity Object containing the most demanding companies for jobs.

![](https://github.com/December-peony/WuzzufJobsWebAnalysis/blob/master/src/main/resources/static/Companies.png)

### Popular Job Titles function:

access : <a>http://localhost:3030/PopularJobTitles</a>
* Returns a ResponseEntity Object containing the most popular job titles.

![](https://github.com/December-peony/WuzzufJobsWebAnalysis/blob/master/src/main/resources/static/Jobs.png)

### Popular Areas function:

access : <a>http://localhost:3030/PopularAreas</a>
* Returns a ResponseEntity Object containing  the most popular areas.

![](https://github.com/December-peony/WuzzufJobsWebAnalysis/blob/master/src/main/resources/static/Areas.png)

### Popular Skills function:

access : <a>http://localhost:3030/PopularSkills</a>
* Returns a ResponseEntity Object containing the most required skills.
* Showing on server as json.
* Ps: you can get more than the top ten skills by changing the size sent to filterBySkills function in Analyzer Class.

![](https://github.com/December-peony/WuzzufJobsWebAnalysis/blob/master/out/Skills.png)


### Years of experience factrozatied function:

access : <a>http://localhost:3030/YearsExp</a>
* Returns a ResponseEntity Object containing two columns, the first is the years of experience, the second how many it's required.

![](https://github.com/December-peony/WuzzufJobsWebAnalysis/blob/master/out/Years.png)


### Analysis function:

access : <a>http://localhost:3030/Analysis</a>
* Model and view analysis html contains the three generated charts as a sum up.

### Future Updates:
* Finishing WuzzafKMeans Model that clusters the job titles and the company names into the suitable K vaule.

* We have tried to creat a KMeans Class and to find the K value of the elbow bend point, but we didn't have enough time to finish this part.

* We have also tried to Create Scatter Plot Class using jfree. 

* We faced many difficulties and didn't have enough time to solve the problems we faced that stopped us from having the required output, 
  but we see that we were close to finishing this part of the project and that's why we have it in our future goals to be updated.


## Contributors:
* <a href="https://github.com/saramohey">Sara Mohey</a>
* <a href="https://github.com/Sherry-Younan">Sherry Younan</a>
* <a href="https://github.com/December-peony">Esraa Khaled</a>




