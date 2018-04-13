package web.controllers;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import search.SearchService;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@RestController
public class SearchController {
//  @Autowired
//  SearchService service;

  @RequestMapping("/")
  public String index() {
    return "Index page";
  }

  @RequestMapping("/search")
  public String search() {
    return "Search path";
  }
}
