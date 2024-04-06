package com.comp4321.indexers.posting;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.comp4321.jdbm.SafeHTree;

public class PostingIndex {
    private final String indexName;

    // Inverted index is maintained in sorted order for efficient search
    private final SafeHTree<Integer, Set<Integer>> forwardIndexMap;
    private final SafeHTree<Integer, List<Posting>> invertedIndexMap;

    public PostingIndex(
            String indexName,
            SafeHTree<Integer, Set<Integer>> forwardIndex,
            SafeHTree<Integer, List<Posting>> invertedIndex) {
        this.indexName = indexName;
        this.forwardIndexMap = forwardIndex;
        this.invertedIndexMap = invertedIndex;
    }

    /**
     * Adds a word to the posting index.
     *
     * @param docId The document ID.
     * @param wordId The word ID.
     * @param location The location of the word in the document.
     * @throws IOException If an I/O error occurs.
     */
    public void addWord(Integer docId, Integer wordId, Integer location) throws IOException {
        // Update the forward index
        var forwardWords = forwardIndexMap.get(docId);
        if (forwardWords == null)
            forwardWords = new HashSet<>();
        forwardWords.add(wordId);
        forwardIndexMap.put(docId, forwardWords);

        // Update the inverted index
        var postings = invertedIndexMap.get(wordId);
        if (postings == null)
            postings = new ArrayList<>();

        // Binary search for the posting and add or update it
        final var postingIdx = Collections.binarySearch(postings, new Posting(docId),
                Comparator.comparing(Posting::docId));
        if (0 <= postingIdx && postingIdx < postings.size()) {
            final var curPosting = postings.get(postingIdx);
            curPosting.addLocation(location);

            final var newPosting = curPosting.addLocation(location);
            postings.set(postingIdx, newPosting);
        } else {
            final var newPosting = new Posting(docId, Set.of(location));
            postings.add(-postingIdx - 1, newPosting);
        }
        invertedIndexMap.put(wordId, postings);
    }

    /**
     * Removes a document from the posting index.
     *
     * @param docId the ID of the document to be removed
     * @throws IOException if an I/O error occurs while removing the document
     */
    public void removeDocument(Integer docId) throws IOException {
        // Remove the document from the forward index
        final var forwardWords = forwardIndexMap.get(docId);
        if (forwardWords == null)
            return;
        forwardIndexMap.remove(docId);

        // Remove the document from the inverted index
        for (final var wordId : forwardWords) {
            final var postings = invertedIndexMap.get(wordId);
            if (postings == null)
                throw new IllegalStateException("Inconsistent index");

            final var postingIdx = Collections.binarySearch(postings, new Posting(docId),
                    Comparator.comparing(Posting::docId));
            if (0 <= postingIdx && postingIdx < postings.size()) {
                postings.remove(postingIdx);
                invertedIndexMap.put(wordId, postings);
            } else {
                throw new IllegalStateException("Inconsistent index");
            }
        }
    }

    /**
     * Retrieves the set of forward words associated with the given document ID.
     *
     * @param docId the document ID for which to retrieve the forward words
     * @return the set of forward words associated with the given document ID
     * @throws IOException if an I/O error occurs while retrieving the forward words
     */
    public Set<Integer> getForwardWords(Integer docId) throws IOException {
        return forwardIndexMap.get(docId);
    }

    /**
     * Retrieves the list of postings associated with a given word ID.
     *
     * @param wordId the ID of the word
     * @return the list of postings associated with the word ID
     * @throws IOException if an I/O error occurs while retrieving the postings
     */
    public List<Posting> getPostings(Integer wordId) throws IOException{
        return invertedIndexMap.get(wordId);
    }

    public void printAll() throws IOException {
        System.out.println(indexName + " Forward Index:");
        for (Map.Entry<Integer, Set<Integer>> entry : forwardIndexMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }

        System.out.println(indexName + " Inverted Index:");
        for (Map.Entry<Integer, List<Posting>> entry : invertedIndexMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
    }
}
