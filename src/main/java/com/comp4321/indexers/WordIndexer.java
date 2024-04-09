package com.comp4321.indexers;

import java.io.IOException;
import java.util.Comparator;

import com.comp4321.jdbm.SafeBTree;

import jdbm.RecordManager;

public class WordIndexer {
    public static final String WORD_TO_ID = "wordToId";
    public static final String ID_TO_WORD = "idToWord";

    private final SafeBTree<String, Integer> wordToIdMap;
    private final SafeBTree<Integer, String> idToWordMap;

    public WordIndexer(SafeBTree<String, Integer> wordToIdMap, SafeBTree<Integer, String> idToWordMap) {
        this.wordToIdMap = wordToIdMap;
        this.idToWordMap = idToWordMap;
    }

    public WordIndexer(RecordManager recman) throws IOException {
        this(new SafeBTree<>(recman, WORD_TO_ID, Comparator.<String>naturalOrder()),
                new SafeBTree<>(recman, ID_TO_WORD, Comparator.<Integer>naturalOrder()));
    }

    /**
     * Retrieves the ID associated with the given word. If the word does not exist
     * in the wordToIdMap, a new ID is created and associated with the word.
     * We don't support removing words from the wordToIdMap, since we cannot
     * guarantee that the ID associated with the word will not be reused.
     *
     * @param word the word to retrieve the ID for
     * @return the ID associated with the word
     * @throws IOException if an error occurs while accessing the wordToIdMap
     */
    public Integer getOrCreateId(String word) throws IOException {
        final var value = wordToIdMap.find(word);
        if (value != null)
            return value;

        final var id = wordToIdMap.size() + 1;
        wordToIdMap.insert(word, id);
        idToWordMap.insert(id, word);
        return id;
    }

    /**
     * Retrieves the word associated with the given ID.
     *
     * @param id the ID of the word to retrieve
     * @return the word associated with the given ID
     * @throws IOException if an I/O error occurs while retrieving the word
     */
    public String getWord(Integer id) throws IOException {
        return idToWordMap.find(id);
    }

    public void printAll() {
        System.out.println("WORD_TO_ID:");
        for (final var entry : wordToIdMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("ID_TO_WORD:");
        for (final var entry : idToWordMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }
}
