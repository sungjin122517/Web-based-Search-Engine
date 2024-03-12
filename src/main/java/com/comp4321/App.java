package com.comp4321;

import java.io.IOException;

import org.htmlparser.util.ParserException;

import com.comp4321.indexers.Indexer;

public class App {
    public static void main(String[] arg) {
        final var baseURL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm";
        final var maxPages = 30;

        try (final var indexer = new Indexer(maxPages)) {
            final var crawler = new Crawler(baseURL);
            crawler.bfs(maxPages, indexer::indexDocument);
            indexer.printAll();

        } catch (ParserException | IOException e) {
            e.printStackTrace();
        }

    }
}
