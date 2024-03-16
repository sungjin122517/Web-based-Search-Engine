package com.comp4321.indexers;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.Objects;

public record Metadata(String title, ZonedDateTime lastModified, Long pageSize) implements Serializable {
    public Metadata {
        Objects.requireNonNull(title);
        Objects.requireNonNull(lastModified);
        Objects.requireNonNull(pageSize);
    }
}