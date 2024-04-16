package com.comp4321.server;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.comp4321.SearchResult;

public record ResultPage(Set<String> keywords, List<String> phrase, List<SearchResult> results) {
    public ResultPage {
        Objects.requireNonNull(keywords);
        Objects.requireNonNull(phrase);
        Objects.requireNonNull(results);
    }
}
