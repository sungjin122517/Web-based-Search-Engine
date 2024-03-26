package com.comp4321.indexers;

public class IndexerException extends RuntimeException {
    public IndexerException(String message) {
        super(message);
    }

    public IndexerException(String message, Throwable cause) {
        super(message, cause);
    }
}
