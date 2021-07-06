package com.example.WuzzufJobsWebAnalysis;

/*----------------------------------------------------
[File Name]: AnalyzerWebService.java
[Authors]: Esraa , Sara ,Sherry
------------------------------------------------------*/

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/*---------------------------------------------------------------------
[Class Name]: AnalyzerWebService
[Description]: Gets the cleaned data and send it to server
               accessed by get request.

---------------------------------------------------------------------*/
@RestController
public class AnalyzerWebService {


    //Attributes are static to save the data and not init;ize it everytime with null
    static Map<String, Long> skills;
    static Map<String, Long> titles;
    static Map<String, Long> areas;
    static Map<String, Long> years;
    static Map<String, Long> companies;

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
        return model;



    }
    @RequestMapping("/PopularSkills")
    public ResponseEntity<Object> skills() {
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
