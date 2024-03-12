package com.comp4321;

import org.htmlparser.util.ParserException;

public class App {
    public static void main(String[] arg) {
        String baseURL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm";
        try {
            final var crawler = new Crawler(baseURL);
            final var bfsResult = crawler.bfs(1000);
            System.out.println("Total pages: " + bfsResult.size());
            bfsResult.stream().forEach(System.out::println);
        } catch (ParserException e) {
            System.err.println(e.toString());
        }
    }
}
