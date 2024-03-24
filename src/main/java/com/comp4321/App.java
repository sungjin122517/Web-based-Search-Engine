package com.comp4321;

import java.io.IOException;

import org.htmlparser.util.ParserException;

import com.comp4321.indexers.Indexer;

public class App {
    public static void main(String[] args) {
        final var baseURL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm";
        final var maxPages = 30;

        if (args.length > 0) {
            try (final var indexer = new Indexer()) {
                final var crawler = new Crawler(baseURL);

                switch (args[0]) {
                    case "crawl":
                        crawler.bfs(maxPages, indexer::indexDocument);
                        break;
                    case "spider_result":
                        indexer.outputSpiderResult("spider_result.txt");
                        break;
                    default:
                        System.out.println("Unknown argument. Please use 'crawl' or 'spider_result'");
                        break;
                }
            } catch (ParserException | IOException e) {
                e.printStackTrace();
            }
        } else {
            System.out.println("Please provide an argument. Use 'crawl' or 'spider_result'");
        }        
    }
}
