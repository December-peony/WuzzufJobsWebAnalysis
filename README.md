# Wuzzuf Jobs Web Analysis

Reading, cleaning wuzzuf(Egyptain jobs website) data set and produce statistics about job market in Egypt.\
dataset source : <a>https://www.kaggle.com/omarhanyy/wuzzuf-jobs</a> \
Project is done using java, displaying results as json on a spring boot web serivce. 
* This project is a final project for java module at AI track-ITI Egypt. 




## Data reading and cleaning:

* For data reading we used spark rdd.
* For cleaning, a mix between rdd and tablesaw. 

### filterTable function:

* Function takes column name and size as input.
* Work on the sent column { counting by value , sorting }
* Returns A table of two columns, the first column is the Category and the second is the count, reduced to the sent size.
* Ex: size = 10 >> returns only 10 first rows.

### filterByExperience function:

* Function takes size as input.
*
*


### filterBySkills function:

* Function takes size as input.
*
*

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

### Years of experience factrozatied function:

access : <a>http://localhost:3030/YearsExp</a>
* Returns a ResponseEntity Object containing two columns, the first is the years of experience, the second how many it's required.


### Analysis function:

access : <a>http://localhost:3030/Analysis</a>
* Model and view analysis html contains the three generated charts as a sum up.

## Contributors:
* <a href="https://github.com/saramohey">Sara Mohey</a>
* <a href="https://github.com/Sherry-Younan">Sherry Younan</a>
* <a href="https://github.com/December-peony">Esraa Khaled</a>




