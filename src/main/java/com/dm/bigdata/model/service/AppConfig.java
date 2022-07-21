package com.dm.bigdata.model.service;

import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.web.SecurityFilterChain;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class AppConfig {

    static final Logger LOGGER = Logger.getLogger(AppConfig.class.getName());

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
}
