package com.dm.bigdata.model.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class AppConfig {

    static final Logger LOGGER = Logger.getLogger(AppConfig.class.getName());

    @Value("${spring.datasource.url}")
    transient String springDatasourceUrl;
    @Value("${spring.datasource.username}")
    transient String springDatasourceUsername;
    @Value("${spring.datasource.password}")
    transient String springDatasourcePassword;

    /**
     * For Json parsing
     * 
     * @return
     */
    @Bean
    public ObjectMapper jsonMapper() {
        return new ObjectMapper();
    }

    /**
     * Http Security
     * 
     * @param http
     * @return
     * @throws Exception
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http.cors().and().csrf().disable().authorizeHttpRequests(
                (authz) -> authz.anyRequest()
                        .permitAll());

        return http.build();
    }

    @Bean
    public Connection jdbc() throws SQLException{
        return DriverManager.getConnection(this.springDatasourceUrl, this.springDatasourceUsername, this.springDatasourcePassword);
    }
}
