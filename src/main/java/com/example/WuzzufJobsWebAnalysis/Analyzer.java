package com.example.WuzzufJobsWebAnalysis;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import tech.tablesaw.api.Table;

import java.io.IOException;
import java.util.LinkedHashMap;

public class Analyzer {

    private LinkedHashMap<String, Long> skills;
    private LinkedHashMap<String, Long> titles;
    private LinkedHashMap<String, Long> company;
    private LinkedHashMap<String, Long> area;
    private LinkedHashMap<String, Long> YearsExp;

    public Analyzer() throws IOException {
        Logger.getLogger("org").setLevel(Level.ERROR);
        DAO t = new DAO();
        t.DAO("in/Wuzzuf_Jobs.csv");
        t.sumUp();
        this.skills = t.filterBySkills(10);
        this.titles = t.filterByTitle(10);
        this.company = t.filterByCompany(10);
        this.area = t.filterByArea(10);
        //System.out.println(t.filterBySkills(10).print());
        this.YearsExp= t.filterByExperience(10);
        Table tbl =t.get_DataTable();
        System.out.println(tbl.print(10));
        System.out.println(tbl.structure());

    }

    public LinkedHashMap<String, Long> getSkills(){
        //System.out.println("Help");
        //System.out.println(this.skills);
        return this.skills;
    }
    public LinkedHashMap<String, Long> getTitles(){
        //System.out.println("Help");
        //System.out.println(this.skills);
        return this.titles;
    }
    public LinkedHashMap<String, Long> getCompany(){
        //System.out.println("Help");
        //System.out.println(this.skills);
        return this.company;
    }
    public LinkedHashMap<String, Long> getAreas(){
        //System.out.println("Help");
        //System.out.println(this.skills);
        return this.area;
    }

    public LinkedHashMap<String, Long> getYearsExp(){
        //System.out.println("Help");
        //System.out.println(this.skills);
        return this.YearsExp;
    }
}

