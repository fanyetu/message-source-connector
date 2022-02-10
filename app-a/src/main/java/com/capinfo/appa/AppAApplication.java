package com.capinfo.appa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EntityScan(basePackages = "com.capinfo")
@EnableJpaRepositories(basePackages = "com.capinfo")
@ComponentScan(basePackages = "com.capinfo")
public class AppAApplication {

	public static void main(String[] args) {
		SpringApplication.run(AppAApplication.class, args);
	}

}
