package com.comp4321;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.comp4321.indexers.Indexer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

public class Server {
    private final ExecutorService executor;
    private final HttpServer httpServer;
    private final Indexer indexer;
    private final int maxSearchResults;

    public static final String SEARCH_PATH = "/";
    public static final String RESULT_PATH = "/result";

    public Server(Indexer indexer, int maxSearchResults) throws IOException {
        this.executor = Executors.newSingleThreadExecutor();
        this.httpServer = HttpServer.create(new InetSocketAddress(0), 5);
        this.indexer = indexer;
        this.maxSearchResults = maxSearchResults;

        httpServer.createContext("/", this::handler);
    }

    public void start() {
        httpServer.setExecutor(executor);
        httpServer.start();

        final var port = httpServer.getAddress().getPort();
        System.out.println("Server started at http://localhost:" + port + "/");

        // Block until the server is stopped or interrupted
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void handler(HttpExchange exchange) throws IOException {
        final var path = exchange.getRequestURI().getPath();
        if (path.equals(SEARCH_PATH))
            searchHandler(exchange);
        else if (path.equals(RESULT_PATH))
            resultHandler(exchange);
        else
            catchAllHandler(exchange);
    }

    private void catchAllHandler(HttpExchange exchange) throws IOException {
        // Respond with 404
        final var error = "404 Not Found";
        exchange.sendResponseHeaders(404, error.length());
        exchange.getResponseBody().write(error.getBytes());
        exchange.close();
    }

    private void searchHandler(HttpExchange exchange) throws IOException {
        // Respond with the index html
        try (final var html = getClass().getClassLoader().getResourceAsStream("index.html")) {
            final var response = html.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", "text/html");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } finally {
            exchange.close();
        }
    }

    private void resultHandler(HttpExchange exchange) throws IOException {
        // Reject if no query
        final var query = exchange.getRequestURI().getQuery();
        if (query == null) {
            final var error = "400 Bad Request: Missing query";
            exchange.sendResponseHeaders(400, error.length());
            exchange.getResponseBody().write(error.getBytes());
            exchange.close();
            return;
        }

        // Parse query and collect search keywords
        // TODO: Parse phrase search keywords
        final var queryList = Pattern.compile("\\s*&\\s*")
                .splitAsStream(query)
                .map(s -> s.split("=", 2))
                .collect(Collectors.toMap(param -> param[0], param -> param.length > 1 ? param[1] : "",
                        (x, y) -> String.join(" ", x, y)));
        final var keywords = Arrays.stream(
                queryList.getOrDefault("search",
                        "").split("[^a-zA-Z0-9_-]+"))
                .filter(s -> !s.isBlank())
                .collect(Collectors.toSet());

        // Reject if no keywords
        if (keywords.isEmpty()) {
            final var error = "400 Bad Request: Missing search keywords";
            exchange.sendResponseHeaders(400, error.length());
            exchange.getResponseBody().write(error.getBytes());
            exchange.close();
            return;
        }

        final var results = indexer.search(keywords, List.of()).entrySet().stream()
                .sorted(Comparator.comparing(entry -> entry.getValue().score(),
                        Comparator.reverseOrder()))
                .limit(maxSearchResults)
                .map(entry -> entry.getValue().toHTML())
                .toList();

        try (final var html = getClass().getClassLoader().getResourceAsStream("result.html")) {
            final var response = new String(html.readAllBytes(), StandardCharsets.UTF_8);
            final var responseBody = response.replace("{{target}}",
                    results.isEmpty()
                            ? "No results found"
                            : String.join("", results))
                    .getBytes();

            exchange.getResponseHeaders().set("Content-Type", "text/html");
            exchange.sendResponseHeaders(200, responseBody.length);
            exchange.getResponseBody().write(responseBody);
        } finally {
            exchange.close();
        }
    }
}