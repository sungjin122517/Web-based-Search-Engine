package com.comp4321;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface SearchEngine {
    public Map<Integer, SearchResult> search(Set<String> words, List<String> phrase) throws IOException;

    public Optional<String> stemWord(String word);
}
