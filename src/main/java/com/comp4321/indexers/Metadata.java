package com.comp4321.indexers;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;

public record Metadata(ZonedDateTime lastModified) implements Serializable {
    public Metadata {
        Objects.requireNonNull(lastModified);
    }
}