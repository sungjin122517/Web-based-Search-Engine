package com.comp4321.jdbm;

public class JDBMException extends RuntimeException {
    public JDBMException(String message) {
        super(message);
    }

    public JDBMException(String message, Throwable cause) {
        super(message, cause);
    }
}
