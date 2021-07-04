package com.example.WuzzufJobsWebAnalysis;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;

import java.io.IOException;


//open http://localhost:3030/Analysis to see
@SpringBootApplication
public class WuzzufJobsWebAnalysisApplication {

	public static void main(String[] args) throws IOException {

		//SpringApplication.run(WuzzufJobsWebAnalysisApplication.class, args);
		SpringApplicationBuilder builder = new SpringApplicationBuilder(WuzzufJobsWebAnalysisApplication.class);

		builder.headless(false);

		ConfigurableApplicationContext context = builder.run(args);
		AnalyzerWebService a = new AnalyzerWebService();
		a.AnalyzerWebService();
	}

	//@Override


}
