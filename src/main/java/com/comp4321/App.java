package com.comp4321;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.htmlparser.util.ParserException;

import com.comp4321.indexers.Indexer;

public class App {
    public static void main(String[] args) throws IOException {

        if (args.length == 0) {
            System.err.println("Please provide an argument. Use 'crawl' or 'search <words>'");
            System.exit(1);
        }

        final var baseURL = "https://www.cse.ust.hk/~kwtleung/COMP4321/testpage.htm";
        final var maxPages = 300;

        try (final var indexer = new Indexer()) {
            switch (args[0]) {
                case "crawl":
                    indexer.bfs(baseURL, maxPages);
                    break;

                case "search":
                    final var maxSearchResults = 50;

                    final var words = Arrays.stream(args).skip(1).collect(Collectors.toSet());
                    final var results = indexer.search(words, List.of());
                    results.entrySet().stream()
                            .sorted((a, b) -> Double.compare(b.getValue().score(), a.getValue().score()))
                            .limit(maxSearchResults)
                            .forEach(entry -> System.out.println(entry.getValue().toResultFormat()));
                    break;

                case "print":
                    indexer.printAll();
                    break;

                default:
                    System.out.println("Unknown argument. Use 'crawl' or 'search <words>'");
                    System.exit(1);
            }
        } catch (IOException | ParserException e) {
            System.err.println("An error occurred: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
