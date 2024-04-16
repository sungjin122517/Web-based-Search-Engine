package com.comp4321;

import io.javalin.Javalin;

public class JavalinServer {
    private final Javalin app;

    public JavalinServer() {
        app = Javalin.create();
    }

    public void start() {
        app.start(8080);
    }
}
