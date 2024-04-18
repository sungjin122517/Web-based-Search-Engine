package com.comp4321.indexers;

import java.io.Serializable;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public record Posting(Integer docId, Set<Integer> titleLocations, Set<Integer> bodyLocations) implements Serializable {
    public Posting {
        Objects.requireNonNull(docId);
        Objects.requireNonNull(titleLocations);
        Objects.requireNonNull(bodyLocations);
        if (docId < 0)
            throw new IllegalArgumentException("docId must be non-negative");
    }

    /**
     * Constructs an empty Posting object with the given document ID.
     * The term frequency is set to 0 and the positions set is empty.
     *
     * @param docId the document ID of the posting
     */
    public Posting(Integer docId) {
        this(docId, Set.of(), Set.of());
    }

    /**
     * Adds a title location to the posting.
     * 
     * @param titleLocation The location of the term within the document's title.
     * @return A new Posting object with the added title location.
     */
    public Posting addTitleLocation(Integer titleLocation) {
        final var newTitleLocations = new TreeSet<>(titleLocations);
        newTitleLocations.add(titleLocation);
        return new Posting(docId, Collections.unmodifiableSet(newTitleLocations), bodyLocations);
    }

    /**
     * Adds a body location to the posting.
     * 
     * @param bodyLocation The location of the term within the document's body.
     * @return A new Posting object with the added body location.
     */
    public Posting addBodyLocation(Integer bodyLocation) {
        final var newBodyLocations = new TreeSet<>(bodyLocations);
        newBodyLocations.add(bodyLocation);
        return new Posting(docId, titleLocations, Collections.unmodifiableSet(newBodyLocations));
    }
}
