package com.comp4321.server;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.comp4321.SearchEngine;
import com.comp4321.SearchResult;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.rendering.template.JavalinJte;

public class JavalinServer {
        private final SearchEngine engine;
        private final int maxSearchResults;

        private final Javalin app;
        private final Semaphore isServerStopped;

        public JavalinServer(SearchEngine engine, int maxSearchResults) {
                this.engine = engine;
                this.maxSearchResults = maxSearchResults;
                this.isServerStopped = new Semaphore(0);

                app = Javalin.create(config -> config.fileRenderer(new JavalinJte()));
                app.get("/", this::renderIndexPage);
                app.get("/result", this::renderResultPage);

                Runtime.getRuntime().addShutdownHook(new Thread(app::stop));
                app.events(event -> event.serverStopped(isServerStopped::release));

        }

        private void renderIndexPage(Context ctx) {
                ctx.render("index.jte");
        }

        private void renderResultPage(Context ctx) throws IOException {
                final var query = ctx.queryParam("search");

                final var keywords = Arrays.stream(query.split("[^a-zA-Z0-9_-]+"))
                                .filter(s -> !s.isBlank())
                                .collect(Collectors.toSet());
                final var keywordStems = keywords.stream()
                                .map(engine::stemWord)
                                .flatMap(Optional::stream)
                                .collect(Collectors.toSet());

                final var phrasePat = Pattern.compile("\"(.*)\"").matcher(query);
                final var phrase = phrasePat.find()
                                ? Arrays.stream(phrasePat.group(1).split("[^a-zA-Z0-9_-]+"))
                                                .filter(s -> !s.isBlank())
                                                .collect(Collectors.toList())
                                : List.<String>of();
                final var phraseStems = phrase.stream()
                                .map(engine::stemWord)
                                .flatMap(Optional::stream)
                                .collect(Collectors.toList());

                final var searchResults = engine.search(keywords, phrase)
                                .values().stream()
                                .sorted(Comparator.comparing(SearchResult::score, Comparator.reverseOrder()))
                                .limit(maxSearchResults)
                                .toList();

                ctx.render("result.jte", Collections.singletonMap("page",
                                new ResultPage(keywordStems, phraseStems, searchResults)));
        }

        public void start() {
                app.start();
        }

        public void awaitTermination() throws InterruptedException {
                isServerStopped.acquire();
        }
}
