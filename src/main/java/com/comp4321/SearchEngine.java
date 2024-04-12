package com.comp4321;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@FunctionalInterface
public interface SearchEngine {
    public Map<Integer, SearchResult> search(Set<String> words, List<String> phrase) throws IOException;
}
