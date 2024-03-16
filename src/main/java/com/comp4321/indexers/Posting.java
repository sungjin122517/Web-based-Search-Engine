package com.comp4321.indexers;

import java.io.Serializable;
import java.util.Objects;

public record Posting(Integer docId, Integer frequency) implements Serializable {
    public Posting {
        Objects.requireNonNull(docId);
        Objects.requireNonNull(frequency);
        if (docId < 0 || frequency < 0)
            throw new IllegalArgumentException("docId and frequency must be non-negative");
    }
}
