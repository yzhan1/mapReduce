package web.controllers;

import models.Article;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import search.SearchService;

import java.io.IOException;
import java.util.List;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@Controller
public class SearchController {
    @Autowired
    SearchService service;

    @GetMapping(value = "/")
    public String index() {
        return "index";
    }

    @GetMapping(value = "/search")
    public String search(@RequestParam("query") String query, Model model) throws IOException, InterruptedException {
        List<Article> results = service.search(query);
        model.addAttribute("results", results);
        return "index";
    }
}
