package es.unizar.tmdad.lab2.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import es.unizar.tmdad.lab2.service.TwitterLookupService;

@Controller
public class SearchController {

    @Autowired
    TwitterLookupService twitter;

    @RequestMapping("/")
    public String greeting() {
        return "index";
    }
    
    @MessageMapping("/search")
	public void search(String query) {
		twitter.search(query);
	}
}