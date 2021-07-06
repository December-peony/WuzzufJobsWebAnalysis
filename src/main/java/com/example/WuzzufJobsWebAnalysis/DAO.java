package com.example.WuzzufJobsWebAnalysis;
/*----------------------------------------------------
[File Name]: DAO.java
[Authors]: Esraa , Sara ,Sherry
------------------------------------------------------*/
//imports
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import tech.tablesaw.api.*;


import java.io.IOException;
import java.util.*;

/*---------------------------------------------------------------------
[Class Name]: DAO
[Description]: Reads csv file as table and RDD.
               Cleans,filters and visualize data.
[Attributes]:
* private Table DataSetTable: tablesaw table holding the whole dataset
* private JavaRDD<String> jobs: String RDD holding the whole dataset
* public SparkConf conf: spark configuration object
* public JavaSparkContext sc: spark session context
[Methods]:
* DAO: class constructor
* getDataSetTable: DataSetTable getter function
* getJobs: jobs getter function
* sumUp: prints DataSetTable summary and structure
* filterByTitle: get top demanding jobs
* filterByCompany: get top demanding companies
* filterByArea: get top demanding areas
* filterBySkills: get top demanding skills
* filterByExperience: get top demanding years of experience
* makeBarChart: creates bar chart
* makePieChart: creates pie chart
* filterTable: get top ten demanding for any given table name
* MakeMapFromRDD: converts RDD to LinkedHashMap
* MakeMapFromLists: converts two lists to LinkedHashMap<list1,list2>
---------------------------------------------------------------------*/

public class DAO {

    private Table DataSetTable;
    private JavaRDD<String> jobs;
    SparkConf conf = new SparkConf().setAppName("WuzzufJobs").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);
    /*---------------------------------------------------------------------
    * [Function Name]: DAO
    * [Description]: This function is responsible for reading the csv file
    *                and forming (RDD , Table) holding it's data
    * [Args]:
    *   String file_path: Holds the path of csv file as String
    * [Returns]: void
    ---------------------------------------------------------------------*/
    public void DAO(String file_path){
        //read csv file into jobs RDD,remove duplicates,remove rows with null and
        //incorrect data,remove the header
        jobs = sc.textFile(file_path)
                .distinct()
                .filter(line -> !line.contains("null"))
                .filter(line-> ((line.split("\"")[0]).split(",").length==7))
                .filter(line -> !line.contains("Title"));
        //create a string tablesaw columns for each data column
        StringColumn title=StringColumn.create("Title");
        StringColumn company=StringColumn.create("Company");
        StringColumn location=StringColumn.create("Location");
        StringColumn type=StringColumn.create("Type");
        StringColumn level=StringColumn.create("Level");
        StringColumn yExp=StringColumn.create("Years of Experience");
        StringColumn country=StringColumn.create("Country");
        StringColumn skills=StringColumn.create("Skills");
        //create a set of all Egyptian cities included in the data
        Set<String> egyptSet = new HashSet<>(Arrays.asList("Egypt","New Valley","Aswan","Assiut","Beni Suef","Beheira","Cairo",
                "Giza","Alexandria","Damietta","South Sinai","Suez","Fayoum","Matruh","Sharqia","Dakahlia"
                ,"Qena","Minya","Qalubia","Red Sea","Gharbia","Monufya","Ismailia"));
        //Split columns in entries of RDD and append the values to the created columns
        for (String job:jobs.collect()){
            String[] parts = job.split("\"");
            String[] lst = parts[0].split(",");
            title.append(lst[0].trim().split(" *[-(/&]")[0]);
            company.append(lst[1].trim());
            location.append(lst[2].trim());
            type.append(lst[3].trim());
            level.append(lst[4].trim());
            yExp.append(lst[5].trim());
            //if the country field is written as an Egyptian city(check set)
            //replace it with Egypt
            if(egyptSet.contains(lst[6].trim()))
                country.append("Egypt");
            else
                country.append(lst[6].trim());
            skills.append(parts[1].trim());
        }
        //wrap all columns into one table and assign the table to DataSetTable attribute
        DataSetTable=Table.create("Wuzzuf Data").addColumns(title,company,location,type,level,yExp,country,skills);
    }
    /*---------------------------------------------------------------------
    * [Function Name]: getDataSetTable
    * [Description]: DataSetTable Attribute getter
    * [Args]:No arguments
    * [Returns]: Table DataSetTable
    ---------------------------------------------------------------------*/
    public Table getDataSetTable() { return DataSetTable; }
    /*---------------------------------------------------------------------
    * [Function Name]: getJobs
    * [Description]: jobs attribute getter
    * [Args]:No arguments
    * [Returns]: RDD<String> jobs
    ---------------------------------------------------------------------*/
    public JavaRDD<String> getJobs() { return jobs; }
    /*---------------------------------------------------------------------
    * [Function Name]: sumUp
    * [Description]: This function is responsible for printing
    *                   summary and structure of DataSetTable
    *                   in the console
    * [Args]:No arguments
    * [Returns]: void
    ---------------------------------------------------------------------*/
    public void sumUp() {

        System.out.println(DataSetTable.structure());
        System.out.println(DataSetTable.summary());
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterByTitle
    * [Description]: Prints the top demanding jobs as a table-saw Table
    *                Graphs job title vs demands
    *                returns the table as LinkedHashMap
    * [Args]:
    *     int size: size of filtered data
    * [Returns]: LinkedHashMap map
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> filterByTitle(int size) throws IOException {
        //creates a table with columns(title , count),sorted descendingly by count.
        Table topTitles = filterTable( "Title", size);
        //prints the table.
        System.out.println(topTitles.print());
        //extracts table columns as lists.
        List title= Arrays.asList(topTitles.column("Title").asStringColumn().asObjectArray());
        List count= Arrays.asList(topTitles.column("Count").asObjectArray());
        //Visualize the data as bar chart.
        makeBarChart(title,count , "Jobs' Popularity" , "Jobs");
        //Returns a LinkedHashMap version of the table .
        LinkedHashMap<String, Long> map = MakeMapFromLists(title,count);
        return map;
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterByCompany
    * [Description]: Prints the top job demanding companies as a table-saw Table
    *                Creates a pie chart of companies demand
    *                returns the table as LinkedHashMap
    * [Args]:
    *     int size: size of filtered data
    * [Returns]: LinkedHashMap map
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> filterByCompany(int size) throws IOException {
        //creates a table with columns(company , count),sorted descendingly by count.
        Table topCompanies = filterTable( "Company", size);
        //prints the table.
        System.out.println(topCompanies.print());
        //extracts table columns as lists.
        List company= Arrays.asList(topCompanies.column("Company").asStringColumn().asObjectArray());
        List count= Arrays.asList(topCompanies.column("Count").asObjectArray());
        //Visualize the data as pie chart.
        makePieChart(company,count , "Popular Companies" , "Companies");
        //Creates a LinkedHashMap version of the table .
        LinkedHashMap<String, Long> map = MakeMapFromLists(company,count);
        return map;
        //return topCompanies;
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterByArea
    * [Description]: Prints the top demanding Areas as a table-saw Table
    *                Graphs Areas vs demands
    *                returns the table as LinkedHashMap
    * [Args]:
    *     int size: size of filtered data
    * [Returns]: LinkedHashMap map
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> filterByArea(int size) throws IOException {
        //creates a table with columns(Area , count),sorted descendingly by count.
        Table topAreas = filterTable( "Location", size);
        //prints the table
        System.out.println(topAreas.print());
        //extract table columns as lists
        List Areas= Arrays.asList(topAreas.column("Location").asStringColumn().asObjectArray());
        List count= Arrays.asList(topAreas.column("Count").asObjectArray());
        //Visualize the relation between Areas and job demand as bar chart
        makeBarChart(Areas,count , "Areas' Popularity" , "Areas");
        //Creates a LinkedHashMap version of the table .
        LinkedHashMap<String, Long> map = MakeMapFromLists(Areas,count);
        return map;
        //return topAreas;
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterByArea
    * [Description]: Prints the top demanding Areas as a table-saw Table
    *                Graphs Areas vs demands
    *                returns the table as LinkedHashMap
    * [Args]:
    *     int size: size of filtered data
    * [Returns]: LinkedHashMap map
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long>  filterBySkills(int size){
        //get the skills part of the job RDD entries
        JavaRDD<String> skills= jobs.map(line -> line.split("\"")[1]);
        //join all skills with commas , split them again using comma and create
        //a String RDD from the splited array
        JavaRDD<String> ListedSkills = sc.parallelize(Arrays.asList(((skills.reduce((a, b) -> a+','+b)).split(","))))
                .map(s -> s.trim());
        //create a skills column holding all skills and get it's value from the created RDD
        StringColumn skillsColumn =  StringColumn.create("skills");
        for (String skill :ListedSkills.collect()){
            skillsColumn.append(skill);
        }
        //add the column to a new table
        Table t =Table.create("skillTable").addColumns(skillsColumn);
        //sort the new table descendingly according to skills' frequency
        Table skillsTable= t.countBy("skills").sortDescendingOn("Count").first(size);
        skillsTable.column("Category").setName("Skills");
        skillsTable.setName("Popular Skills");
        //print the new table (skills , count)
        System.out.println(skillsTable.print());
        //extract lists from the table
        List Skills= Arrays.asList(skillsTable.column("Skills").asStringColumn().asObjectArray());
        List count= Arrays.asList(skillsTable.column("Count").asObjectArray());
        //Creates a LinkedHashMap version of the table .
        LinkedHashMap<String, Long> map = MakeMapFromLists(Skills,count);
        return map;
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterByExperience
    * [Description]: Prints the top job demanding Experiences as a table-saw Table
    *                returns the table as LinkedHashMap
    * [Args]:
    *     int size: size of filtered data
    * [Returns]: LinkedHashMap map
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> filterByExperience(int size){
        //create a list from the jobs RDD of values of years of experience removing
        //the text part and taking only the upper limit of the interval
        //example: 1-3 Yrs Exp -> 1
        //          3+ Yrs Exp -> 3
        List<String> yExper= jobs.map(line -> line.split(",")[5])
                .map(s -> s.trim()).map(s -> (s.split(" ")[0]).split("[-\\+]")[0]).collect();
        //Create a tablesaw column to hold the new format of years of experience
        //parsing the list values as integer
        IntColumn modifiedYExp=IntColumn.create("Numeric YearsExp");
        for (String e :yExper){
            modifiedYExp.append(Integer.parseInt(e));
        }
        //append the modified  years of experience column to the DaraSetTable table
        DataSetTable.addColumns(modifiedYExp);
        //creates a table with columns(Numeric YerasExp , count),sorted descendingly by count.
        Table topYrsExp = filterTable("Numeric YearsExp",size);
        //prints the table:topYrsExp
        System.out.println(topYrsExp .print());
        //prints the first 10 entries of the DatasetTable after modification
        System.out.println(DataSetTable.print(10));
        //Extract the columns of topYrsExp as lists
        List Years= Arrays.asList(topYrsExp.column("Numeric YearsExp").asStringColumn().asObjectArray());
        List count= Arrays.asList(topYrsExp.column("Count").asObjectArray());
        //creates and returns the LinkedHashMap version of the table
        LinkedHashMap<String, Long> map = MakeMapFromLists(Years,count);
        return map;
        //return filterTable("Numeric YearsExp",size);
    }

    /*---------------------------------------------------------------------
    * [Function Name]: makeBarChart
    * [Description]: This function is responsible for making a bar chart,
    *                and saving it as a bitmap image in the folder
    *               "templates"
    * [Args]:
    *   List<String> xList: list of data to be graphed along the horizontal axis
    *   List<String> yList: list of data to be graphed along the vertical axis
    *   String title: Chart title
    *   String XLabel: Label of the horizontal axis
    * [Returns]: void
    ---------------------------------------------------------------------*/
    public static void makeBarChart(List<String> xList,List<Integer> yList , String title , String XLabel) throws IOException {
        //create the chart
        CategoryChart chart = new CategoryChartBuilder().width(1500).height(1000)
                .title(title).xAxisTitle(XLabel).yAxisTitle("Popularity").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries(title,xList,yList);
        //Save the chart as a bitmap image with the same name of the XLabel
        BitmapEncoder.saveBitmap(chart, "src/main/resources/templates/"+XLabel, BitmapEncoder.BitmapFormat.PNG);

    }
    /*---------------------------------------------------------------------
    * [Function Name]: makePieChart
    * [Description]: This function is responsible for making a pie chart,
    *                and saving it as a bitmap image in the folder
    *               "templates"
    * [Args]:
    *   List<String> categories: list of pie chart slices
    *   List<String> values: value of each category(slice)
    *   String title: Chart title
    *   String imageName: name of image file
    * [Returns]: void
    ---------------------------------------------------------------------*/
    public  void makePieChart(List<String> Categories,List<Integer> values, String title , String imageName) throws IOException {
        //Creates the pie chart
        PieChart chart = new PieChartBuilder().width(1000).height(1000).title(title).build();
        for( int i=0;i<Categories.size();i++)
        {
            chart.addSeries(Categories.get(i),values.get(i));
        }
        //Save the chart as a bitmap image with the given file name : imageName
        BitmapEncoder.saveBitmap(chart, "src/main/resources/templates/"+imageName, BitmapEncoder.BitmapFormat.PNG);
    }
    /*---------------------------------------------------------------------
    * [Function Name]: filterTable
    * [Description]: This function is responsible for counting
    *                   DatSetTable entries by a given column
    *                   returning a new table (colName,count) sorted by
    *                   count descendingly.
    * [Args]:
    *   String colName:column to count and sort by
    *   int size: required size of returned table
    * [Returns]: Table t2
    ---------------------------------------------------------------------*/
    public  Table filterTable(String colName,int size){
        Table t= DataSetTable.countBy(colName).sortDescendingOn("Count").first(size);
        t.column("Category").setName(colName);
        Table t2=t.setName(colName+" Popularity");
        return t2;

    }
    /*---------------------------------------------------------------------
    * [Function Name]: MakeMapFromRDD
    * [Description]: This function is responsible for returning a
    *                Sorted descendingly LinkedHashMap version of a RDD
    * [Args]:
    *   JavaRDD Listed: the RDD to convert
    *   int size: the size of converted map
    * [Returns]: LinkedHashMap<String, Long> reverseSortedMap
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> MakeMapFromRDD(JavaRDD Listed,int size){
        //Create a map of value-count
        Map<String, Long> Counts = Listed.countByValue();
        //create an empty LinkedHashMap and insert the values of the Counts list
        //sorted descendingly
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        Counts.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(size)
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
        return reverseSortedMap;

    }
    /*---------------------------------------------------------------------
    * [Function Name]: MakeMapFromLists
    * [Description]: This function is responsible for returning a
    *                Sorted descendingly LinkedHashMap<keys,values> version
    *                from two lists
    * [Args]:
    *   List<String> keys: the keys of the map as a list
    *   List<Long> values: the values of the map as a list
    * [Returns]: LinkedHashMap<String, Long> reverseSortedMap
    ---------------------------------------------------------------------*/
    public LinkedHashMap<String, Long> MakeMapFromLists(List<String> keys,List<Long> values){
        //create an empty LinkedHashMap and insert in it the key value pairs of the
        //given lists on by one.
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        for (int i=0; i<keys.size(); i++) {
            reverseSortedMap.put(keys.get(i), values.get(i));    // is there a clearer way?
        }
        return reverseSortedMap;
    }

}