package com.comp4321.indexers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class PostingIndex {
    public final static String DOCID_TO_TITLEID = "docIdToTitleId";
    public final static String TITLEID_TO_POSTINGS = "titleIdToPostings";
    public final static String DOCID_TO_WORDSID = "docIdToWordsId";
    public final static String WORDSID_TO_POSTINGS = "wordsIdToPostings";

    // Inverted indexes are maintained in sorted order for efficient search
    private final SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap;
    private final SafeHTree<Integer, List<Posting>> titleIdToPostingsMap;
    private final SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap;
    private final SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap;

    public PostingIndex(SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap,
            SafeHTree<Integer, List<Posting>> titleIdToPostingsMap,
            SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap,
            SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap) {
        this.titleIdToPostingsMap = titleIdToPostingsMap;
        this.docIdToTitleIdMap = docIdToTitleIdMap;
        this.wordsIdToPostingsMap = wordsIdToPostingsMap;
        this.docIdToWordsIdMap = docIdToWordsIdMap;
    }

    public PostingIndex(RecordManager recman) throws IOException {
        this(new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_TITLEID),
                new SafeHTree<Integer, List<Posting>>(recman, TITLEID_TO_POSTINGS),
                new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_WORDSID),
                new SafeHTree<Integer, List<Posting>>(recman, WORDSID_TO_POSTINGS));
    }

    private void addWordToIndex(Integer docId, Integer wordId, SafeHTree<Integer, Set<Integer>> forwardIndexMap,
            SafeHTree<Integer, List<Posting>> invertedIndexMap) throws IOException {
        // Update the forward index
        var forwardIndex = forwardIndexMap.get(docId);
        if (forwardIndex == null)
            forwardIndex = new HashSet<>();
        forwardIndex.add(wordId);
        forwardIndexMap.put(docId, forwardIndex);

        // Update the inverted index
        var postings = invertedIndexMap.get(wordId);
        if (postings == null)
            postings = new ArrayList<>();

        // Binary search for the posting and add or update it
        final var postingIdx = Collections.binarySearch(postings, new Posting(docId, 0),
                Comparator.comparing(Posting::docId));
        if (0 <= postingIdx && postingIdx < postings.size()) {
            final var curPosting = postings.get(postingIdx);
            final var newPosting = new Posting(curPosting.docId(), curPosting.frequency() + 1);
            postings.set(postingIdx, newPosting);
        } else {
            final var newPosting = new Posting(docId, 1);
            postings.add(-postingIdx - 1, newPosting);
        }
        invertedIndexMap.put(wordId, postings);
    }

    /**
     * Adds a title to the posting index for a given document.
     *
     * @param docId The ID of the document.
     * @param titleId The ID of the title.
     * @throws RuntimeException if an error occurs while adding the title to the index.
     */
    public void addTitle(Integer docId, Integer titleId) {
        try {
            addWordToIndex(docId, titleId, docIdToTitleIdMap, titleIdToPostingsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a word to the posting index for a given document.
     *
     * @param docId   the ID of the document
     * @param wordId  the ID of the word
     * @throws RuntimeException if an error occurs while adding the word to the index
     */
    public void addWord(Integer docId, Integer wordId) {
        try {
            addWordToIndex(docId, wordId, docIdToWordsIdMap, wordsIdToPostingsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeDocumentFromIndex(Integer docId, SafeHTree<Integer, Set<Integer>> forwardIndexMap,
            SafeHTree<Integer, List<Posting>> invertedIndexMap) throws IOException {
        final var forwardIndex = forwardIndexMap.get(docId);
        if (forwardIndex == null)
            return;
        forwardIndexMap.remove(docId);

        for (final var wordId : forwardIndex) {
            final var postings = invertedIndexMap.get(wordId);
            if (postings == null)
                throw new IllegalStateException("Inconsistent index");

            final var postingIdx = Collections.binarySearch(postings, new Posting(docId, 0),
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
     * Removes a document from the posting index.
     *
     * @param docId the ID of the document to be removed
     * @throws RuntimeException if an error occurs while removing the document
     */
    public void removeDocument(Integer docId) {
        try {
            removeDocumentFromIndex(docId, docIdToTitleIdMap, titleIdToPostingsMap);
            removeDocumentFromIndex(docId, docIdToWordsIdMap, wordsIdToPostingsMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("DOCID_TO_TITLEID:");
        for (final var entry : docIdToTitleIdMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("TITLEID_TO_POSTINGS:");
        for (final var entry : titleIdToPostingsMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("DOCID_TO_WORDSID:");
        for (final var entry : docIdToWordsIdMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("WORDSID_TO_POSTINGS:");
        for (final var entry : wordsIdToPostingsMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }
}
