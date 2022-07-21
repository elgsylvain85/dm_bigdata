package com.dm.bigdata;

// import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration;
// import org.springframework.context.annotation.Import;
// import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
// @EnableScheduling
@EnableAutoConfiguration(exclude = GsonAutoConfiguration.class)
// @EnableBatchProcessing
// @Import(DMBigDataApplication.class)
public class DMBigDataApplication {

	public static void main(String[] args) {
		SpringApplication.run(DMBigDataApplication.class, args);
	}
}
