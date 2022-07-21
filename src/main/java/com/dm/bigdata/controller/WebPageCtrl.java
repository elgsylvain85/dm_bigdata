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

    // @Autowired
    // private ServletContext servletContext;

    @GetMapping(value = "/")
    public String homePage(HttpServletRequest request) throws URISyntaxException {

        // model.addAttribute("appName", this.appName);

        // var path = this.servletContext.getContextPath();
        // http://localhost:9191/
        var pathString = request.getScheme() + "://" + request.getServerName() + ":" + request.getServerPort();

        System.out.println("Current context : " + pathString);

        // model.addAttribute("contextpath", pathString);

        // return "redirect:index.html?contextpath="+path;
        // return new ModelAndView(viewName, model, status)

        // URI uri = URI.create(pathString + "/index.html?contextpath=" + pathString+"&others=bigdata");
        // HttpHeaders httpHeaders = new HttpHeaders();
        // httpHeaders.setLocation(uri);
        // return new ResponseEntity<>(httpHeaders, HttpStatus.SEE_OTHER);
        // return ResponseEntity.status(HttpStatus.FOUND)
        //         .location(uri)
        //         .build();
        return "redirect:"+pathString + "/index.html?contextpath=" + pathString+"&others=bigdata";
    }
}
