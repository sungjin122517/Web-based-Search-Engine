package com.comp4321.indexers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
     * Adds a document to the posting index.
     *
     * @param docId    The ID of the document
     * @param titleIds The list of word IDs in the document's title in order
     * @param bodyIds  The list of word IDs in the document's body in order
     * @throws IOException if an I/O error occurs.
     */
    public void addDocument(Integer docId, List<Integer> titleIds, List<Integer> bodyIds) throws IOException {
        // Update the forward index
        final var totalWords = new HashSet<Integer>(titleIds);
        totalWords.addAll(bodyIds);
        forwardIndexMap.put(docId, totalWords);

        // Update the inverted index
        final var titleLocations = new HashMap<Integer, Set<Integer>>();
        for (int i = 0; i < titleIds.size(); ++i) {
            final var titleId = titleIds.get(i);
            final var locations = titleLocations.getOrDefault(titleId, new HashSet<>());
            locations.add(i);
            titleLocations.put(titleId, locations);
        }

        final var bodyLocations = new HashMap<Integer, Set<Integer>>();
        for (int i = 0; i < bodyIds.size(); ++i) {
            final var bodyId = bodyIds.get(i);
            final var locations = bodyLocations.getOrDefault(bodyId, new HashSet<>());
            locations.add(i);
            bodyLocations.put(bodyId, locations);
        }

        for (final var wordId : totalWords) {
            final var postingToAdd = new Posting(docId, titleLocations.getOrDefault(wordId, new HashSet<>()),
                    bodyLocations.getOrDefault(wordId, new HashSet<>()));

            var postings = invertedIndexMap.get(wordId);
            if (postings == null)
                postings = new ArrayList<>();

            final var postingIdx = Collections.binarySearch(postings, postingToAdd,
                    Comparator.comparing(Posting::docId));
            if (0 <= postingIdx && postingIdx < postings.size()) {
                // It is an error if the posting already exists
                throw new IndexerException(
                        "Posting already exists for word ID " + wordId + " and document ID " + docId);
            } else {
                postings.add(-postingIdx - 1, postingToAdd);
            }
            invertedIndexMap.put(wordId, postings);
        }
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
    public List<Posting> getPostings(Integer wordId) throws IOException {
        return invertedIndexMap.get(wordId);
    }

    /**
     * Retrieves the posting associated with a given document ID and word ID.
     * 
     * @param docId  the ID of the document
     * @param wordId the ID of the word
     * @return the posting associated with the document ID and word ID
     * @throws IOException      if an I/O error occurs while retrieving the posting
     * @throws IndexerException if the posting does not exist
     */
    public Posting getPosting(Integer docId, Integer wordId) throws IOException {
        final var postings = getPostings(wordId);
        if (postings == null)
            throw new IndexerException(indexName + " Inverted Index does not contain word ID " + wordId);

        final var postingIdx = Collections.binarySearch(postings, new Posting(docId),
                Comparator.comparing(Posting::docId));
        if (postingIdx < 0 || postingIdx >= postings.size())
            throw new IndexerException(indexName + " Inverted Index does not contain posting for word ID " + wordId
                    + " and document ID " + docId);

        return postings.get(postingIdx);
    }

    /**
     * Returns the document frequency (DF) of a given word ID.
     *
     * @param wordId the ID of the word
     * @return the document frequency of the word
     * @throws IOException if an I/O error occurs
     */
    public int getDF(Integer wordId) throws IOException {
        return invertedIndexMap.get(wordId).size();
    }

    private List<Posting> mergePhrase(List<Posting> prevPostings, List<Posting> curPostings) {
        final var mergedPostings = new ArrayList<Posting>();

        var prevIdx = 0;
        var curIdx = 0;
        while (prevIdx < prevPostings.size() && curIdx < curPostings.size()) {
            final var prevPosting = prevPostings.get(prevIdx);
            final var curPosting = curPostings.get(curIdx);

            if (prevPosting.docId() < curPosting.docId()) {
                ++prevIdx;
            } else if (prevPosting.docId() > curPosting.docId()) {
                ++curIdx;
            } else {
                final var docId = prevPosting.docId();
                final var newTitleLocations = prevPosting.titleLocations()
                        .stream()
                        .map(loc -> loc + 1)
                        .collect(Collectors.toSet());
                newTitleLocations.retainAll(curPosting.titleLocations());

                final var newBodyLocations = prevPosting.bodyLocations()
                        .stream()
                        .map(loc -> loc + 1)
                        .collect(Collectors.toSet());
                newBodyLocations.retainAll(curPosting.bodyLocations());

                if (!newTitleLocations.isEmpty() || !newBodyLocations.isEmpty())
                    mergedPostings.add(new Posting(docId, newTitleLocations, newBodyLocations));

                ++prevIdx;
                ++curIdx;
            }
        }

        return mergedPostings;
    }

    /**
     * Retrieves the set of documents that contain the given phrase.
     *
     * @param phrase the list of integers representing the phrase
     * @return a set of integers representing the document IDs that contain the
     *         phrase
     * @throws IOException if an I/O error occurs while retrieving the postings
     */
    public Set<Integer> getDocumentsWithPhrase(List<Integer> phrase) throws IOException {
        return phrase.stream()
                .map(wordId -> {
                    try {
                        return getPostings(wordId);
                    } catch (IOException e) {
                        throw new IndexerException("An error occurred while retrieving postings", e);
                    }
                })
                .reduce(this::mergePhrase)
                .stream()
                .flatMap(List::stream)
                .map(Posting::docId)
                .collect(Collectors.toSet());
    }

    public void printAll() {
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
