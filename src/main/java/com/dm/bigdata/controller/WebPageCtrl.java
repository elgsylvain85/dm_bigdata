package com.dm.bigdata.controller;

import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.servlet.ModelAndView;

@Controller
public class WebPageCtrl {

    @Value("${app.name}")
    String appName;

    @GetMapping(value = "/")
    public String homePage(HttpServletRequest request) throws URISyntaxException {

        var pathString = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort();

        System.out.println("Current context : " + pathString);

        return "redirect:" + pathString + "/index.html?contextpath=" + pathString + "&others=bigdata";
    }
}
