package com.comp4321.indexers;

import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public record Posting(Integer docId, Set<Integer> locations) implements Serializable {
    public Posting {
        Objects.requireNonNull(docId);
        Objects.requireNonNull(locations);
        if (docId < 0)
            throw new IllegalArgumentException("docId and frequency must be non-negative");
    }

    /**
     * Constructs an empty Posting object with the given document ID.
     * The term frequency is set to 0 and the positions set is empty.
     *
     * @param docId the document ID of the posting
     */
    public Posting(Integer docId) {
        this(docId, Set.of());
    }

    public Posting addLocation(Integer location) {
        final var newLocations = new TreeSet<>(locations);
        newLocations.add(location);
        return new Posting(docId, Collections.unmodifiableSet(newLocations));
    }
}
