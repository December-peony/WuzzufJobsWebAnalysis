package com.example.WuzzufJobsWebAnalysis;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.LinkedHashMap;

@RestController
public class AnalyzerWebService {

    //Analyzer analyzer = new Analyzer();

    static LinkedHashMap<String, Long> skills;
    static LinkedHashMap<String, Long> titles;
    static LinkedHashMap<String, Long> areas;
    static LinkedHashMap<String, Long> years;
    static LinkedHashMap<String, Long> companies;

    public void AnalyzerWebService() throws IOException {
        Analyzer analyzer = new Analyzer();
        this.skills = analyzer.getSkills();
        this.titles = analyzer.getTitles();
        this.areas = analyzer.getAreas();
        this.years = analyzer.getYearsExp();
        this.companies = analyzer.getCompany();
        System.out.println("Done!");



    }


    @RequestMapping("/Analysis")
    public ModelAndView analysis() throws IOException {


        ModelAndView model = new ModelAndView("analysis.html");
        //model.addObject("company", Arrays.asList("con","LLLL"));
        //model.addObject("number", Arrays.asList(5,6));
        return model;

        //return new ResponseEntity<>("Done", HttpStatus.OK);

    }
    @RequestMapping("/PopularSkills")
    public ResponseEntity<Object> skills() {
        //Object l = this.o.clone();
        return new ResponseEntity<>(skills, HttpStatus.OK);
    }

    @RequestMapping("/PopularJobTitles")
    public ResponseEntity<Object> jobs() {
        return new ResponseEntity<>(titles, HttpStatus.OK);
    }

    @RequestMapping("/PopularAreas")
    public ResponseEntity<Object> Areas() {
        return new ResponseEntity<>(areas, HttpStatus.OK);
    }

    @RequestMapping("/YearsExp")
    public ResponseEntity<Object> yearsOfExp() {
        return new ResponseEntity<>(years, HttpStatus.OK);
    }

    @RequestMapping("/PopularCompanies")
    public ResponseEntity<Object> company1() {
        return new ResponseEntity<>(companies, HttpStatus.OK);
    }


}
