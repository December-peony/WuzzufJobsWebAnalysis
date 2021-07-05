package com.example.WuzzufJobsWebAnalysis;

//import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import tech.tablesaw.api.*;


import java.io.IOException;
import java.util.*;

//Data access object

public class DAO {

    private Table DataSetTable;
    private JavaRDD<String> jobs;
    SparkConf conf = new SparkConf().setAppName("WuzzufJobs").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    public void DAO(String file_path){

        jobs = sc.textFile(file_path)
                .distinct()
                .filter(line -> !line.contains("null"))
                .filter(line-> ((line.split("\"")[0]).split(",").length==7))
                .filter(line -> !line.contains("Title"));
        StringColumn title=StringColumn.create("Title");
        StringColumn company=StringColumn.create("Company");
        StringColumn location=StringColumn.create("Location");
        StringColumn type=StringColumn.create("Type");
        StringColumn level=StringColumn.create("Level");
        StringColumn yExp=StringColumn.create("Years of Experience");
        StringColumn country=StringColumn.create("Country");
        StringColumn skills=StringColumn.create("Skills");
        Set<String> egyptSet = new HashSet<>(Arrays.asList("Egypt","New Valley","Aswan","Assiut","Beni Suef","Beheira","Cairo",
                "Giza","Alexandria","Damietta","South Sinai","Suez","Fayoum","Matruh","Sharqia","Dakahlia"
                ,"Qena","Minya","Qalubia","Red Sea","Gharbia","Monufya","Ismailia"));
        for (String job:jobs.collect()){
            String[] parts = job.split("\"");
            String[] lst = parts[0].split(",");
            title.append(lst[0].trim().split(" *[-(/&]")[0]);
            company.append(lst[1].trim());
            location.append(lst[2].trim());
            type.append(lst[3].trim());
            level.append(lst[4].trim());
            yExp.append(lst[5].trim());
            if(egyptSet.contains(lst[6].trim()))
                country.append("Egypt");
            else
                country.append(lst[6].trim());
            skills.append(parts[1].trim());
        }

        DataSetTable=Table.create("Wuzzuf Data").addColumns(title,company,location,type,level,yExp,country,skills);


    }
    public void sumUp() {

        System.out.println(DataSetTable.structure());
        System.out.println(DataSetTable.summary());
    }
    public Table get_DataTable(){
        return DataSetTable;
    }
    public LinkedHashMap<String, Long> filterByTitle(int size) throws IOException {

        Table topTitles = filterTable( "Title", size);
        List title= Arrays.asList(topTitles.column("Title").asStringColumn().asObjectArray());
        List count= Arrays.asList(topTitles.column("Count").asObjectArray());
        makeBarChart(title,count , "Jobs' Popularity" , "Jobs",size);
        LinkedHashMap<String, Long> map = MakeMapFromLists(title,count);
        return map;
    }
    public LinkedHashMap<String, Long> filterByCompany(int size) throws IOException {
        Table topCompanies = filterTable( "Company", size);
        List company= Arrays.asList(topCompanies.column("Company").asStringColumn().asObjectArray());
        List count= Arrays.asList(topCompanies.column("Count").asObjectArray());
        makePieChart(company,count , "Popular Companies" , "Companies",size);
        LinkedHashMap<String, Long> map = MakeMapFromLists(company,count);
        return map;
        //return topCompanies;
    }
    public LinkedHashMap<String, Long> filterByArea(int size) throws IOException {
        Table topAreas = filterTable( "Location", size);
        List Areas= Arrays.asList(topAreas.column("Location").asStringColumn().asObjectArray());
        List count= Arrays.asList(topAreas.column("Count").asObjectArray());
        makeBarChart(Areas,count , "Areas' Popularity" , "Areas",size);
        LinkedHashMap<String, Long> map = MakeMapFromLists(Areas,count);
        return map;
        //return topAreas;
    }

    public LinkedHashMap<String, Long>  filterBySkills(int size){

        JavaRDD<String> skills= jobs.map(line -> line.split("\"")[1]);
        JavaRDD<String> ListedSkills = sc.parallelize(Arrays.asList(((skills.reduce((a, b) -> a+','+b)).split(","))))
                .map(s -> s.trim());
        StringColumn skillsColumn =  StringColumn.create("skills");
        for (String skill :ListedSkills.collect()){
            skillsColumn.append(skill);
        }
        Table t =Table.create("skillTable").addColumns(skillsColumn);
        Table skillsTable= t.countBy("skills").sortDescendingOn("Count").first(size);
        skillsTable.column("Category").setName("Skills");
        skillsTable.setName("Popular Skills");
        List Skills= Arrays.asList(skillsTable.column("Skills").asStringColumn().asObjectArray());
        List count= Arrays.asList(skillsTable.column("Count").asObjectArray());

        LinkedHashMap<String, Long> map = MakeMapFromLists(Skills,count);
        //LinkedHashMap<String, Long> map = MakeMapFromRDD(ListedSkills,size);


        return map;
    }
    public LinkedHashMap<String, Long> filterByExperience(int size){

        List<String> yExper= jobs.map(line -> line.split(",")[5]).map(s -> s.trim()).map(s -> (s.split(" ")[0]).split("[-\\+]")[0]).collect();
        IntColumn modifiedYExp=IntColumn.create("Numeric YearsExp");
        for (String e :yExper){
            modifiedYExp.append(Integer.parseInt(e));
        }
        DataSetTable.addColumns(modifiedYExp);
        Table t = filterTable("Numeric YearsExp",size);
        List Years= Arrays.asList(t.column("Numeric YearsExp").asStringColumn().asObjectArray());
        List count= Arrays.asList(t.column("Count").asObjectArray());
        LinkedHashMap<String, Long> map = MakeMapFromLists(Years,count);
        return map;
        //return filterTable("Numeric YearsExp",size);
    }


    public static void makeBarChart(List<String> xList,List<Integer> yList , String title , String XLabel,int size) throws IOException {
        CategoryChart chart = new CategoryChartBuilder().width(1500).height(1000).title(title).xAxisTitle(XLabel).yAxisTitle("Popularity").build();
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.addSeries(title,xList,yList);
        //new SwingWrapper<CategoryChart>(chart).displayChart();
        BitmapEncoder.saveBitmap(chart, "out/"+XLabel, BitmapEncoder.BitmapFormat.PNG);

    }

    public  void makePieChart(List<String> xList,List<Integer> yList , String title , String XLabel,int size) throws IOException {
        PieChart chart = new PieChartBuilder().width(1000).height(1000).title("Popular Companies" ).build();
        for( int i=0;i<xList.size();i++)
        {
            chart.addSeries(xList.get(i),yList.get(i));
        }
       // new SwingWrapper<PieChart>(chart).displayChart();
        BitmapEncoder.saveBitmap(chart, "out/"+XLabel, BitmapEncoder.BitmapFormat.PNG);


    }
    public  Table filterTable(String colName,int size){
        Table t= DataSetTable.countBy(colName).sortDescendingOn("Count").first(size);
        t.column("Category").setName(colName);
        Table t2=t.setName(colName+" Popularity");
        return t2;

    }

    public LinkedHashMap<String, Long> MakeMapFromRDD(JavaRDD Listed,int size){
        Map<String, Long> Counts = Listed.countByValue();
        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        Counts.entrySet()
                .stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).limit(size)
                .forEachOrdered(x -> reverseSortedMap.put(x.getKey(), x.getValue()));
        return reverseSortedMap;

    }

    public LinkedHashMap<String, Long> MakeMapFromLists(List<String> keys,List<Long> values){

        LinkedHashMap<String, Long> reverseSortedMap = new LinkedHashMap<>();
        for (int i=0; i<keys.size(); i++) {
            reverseSortedMap.put(keys.get(i), values.get(i));    // is there a clearer way?
        }
        return reverseSortedMap;

    }




}