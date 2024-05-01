package com.comp4321.indexers;

import java.util.List;

public record TestDocument(int docId, List<Integer> titleIds, List<Integer> bodyIds) {

    public static TestDocument titleOnlyDocument = new TestDocument(1, List.of(1, 1, 2, 3), List.of());
    public static TestDocument bodyOnlyDocument = new TestDocument(2, List.of(), List.of(4, 4, 5, 6));
    public static TestDocument mixedDocument = new TestDocument(3, List.of(7, 8, 9), List.of(10, 11, 12));
}