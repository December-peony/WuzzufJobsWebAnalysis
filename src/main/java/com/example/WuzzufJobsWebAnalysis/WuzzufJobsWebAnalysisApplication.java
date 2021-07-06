package com.example.WuzzufJobsWebAnalysis;


import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;


import java.io.IOException;


//open http://localhost:3030/Analysis to see
@SpringBootApplication
public class WuzzufJobsWebAnalysisApplication {

	public static void main(String[] args) throws IOException {


		SpringApplicationBuilder builder = new SpringApplicationBuilder(WuzzufJobsWebAnalysisApplication.class);

		builder.headless(false);

		ConfigurableApplicationContext context = builder.run(args);
		AnalyzerWebService webService = new AnalyzerWebService();
		webService.AnalyzerWebService();
	}



}
