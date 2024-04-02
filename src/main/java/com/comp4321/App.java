package com.comp4321;

import java.io.IOException;

import com.comp4321.indexers.Indexer;

public class App {
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Please provide an argument. Use 'crawl' or 'spider_result'");
            System.exit(1);
        }

        final var baseURL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm";
        final var maxPages = 30;

        try (final var indexer = new Indexer()) {
            switch (args[0]) {
                case "crawl":
                    indexer.bfs(baseURL, maxPages);
                    break;

                case "print":
                    indexer.printAll();
                    break;

                default:
                    System.out.println("Unknown argument. Use 'crawl' or 'spider_result'");
                    System.exit(1);
            }
        }
    }
}
