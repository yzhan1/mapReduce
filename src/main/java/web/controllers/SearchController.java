package web.controllers;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

/**
 * @author Shu Lin Chan, Jonathan Maeda, James Wang, Yaoming Zhan
 * Final Project
 */
@Controller
public class SearchController {
//  @Autowired
//  SearchService service;

    @GetMapping(value = "/")
    public String index() {
        return "index";
    }

    @GetMapping(value = "/search")
    public String search(@RequestParam("query") String query, Model model) {
//    service.search(query);
        model.addAttribute("query", query);
        return "index";
    }
}
