package com.comp4321.indexers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.comp4321.jdbm.SafeBTree;
import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class PostingIndex {
    public final static String DOCID_TO_TITLEID = "docIdToTitleId";
    public final static String TITLEID_TO_POSTINGS = "titleIdToPostings";
    public final static String DOCID_TO_WORDSID = "docIdToWordsId";
    public final static String WORDSID_TO_POSTINGS = "wordsIdToPostings";
    public final static String DOCID_TO_TFMAX = "docIdToTfMax";
    public final static String WORDID_TO_DF = "wordIdToDf";
    public final static String DOCID_TO_LENGTH = "docIdToLength";

    // Inverted indexes are maintained in sorted order for efficient search
    private final SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap;
    private final SafeHTree<Integer, List<Posting>> titleIdToPostingsMap;
    private final SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap;
    private final SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap;
    private final SafeBTree<Integer, Integer> docIdToTFMaxMap;
    private final SafeHTree<Integer, Integer> wordIdToDfMap;
    private final SafeHTree<Integer, Double> docIdToLengthMap;

    // Flag to check if the document lengths have been initialized after indexing
    private boolean isDocLengthInitialized;

    public PostingIndex(SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap,
            SafeHTree<Integer, List<Posting>> titleIdToPostingsMap,
            SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap,
            SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap,
            SafeBTree<Integer, Integer> docIdToTFMaxMap,
            SafeHTree<Integer, Integer> wordIdToDfMap,
            SafeHTree<Integer, Double> docIdToLengthMap) {
        this.titleIdToPostingsMap = titleIdToPostingsMap;
        this.docIdToTitleIdMap = docIdToTitleIdMap;
        this.wordsIdToPostingsMap = wordsIdToPostingsMap;
        this.docIdToWordsIdMap = docIdToWordsIdMap;
        this.docIdToTFMaxMap = docIdToTFMaxMap;
        this.wordIdToDfMap = wordIdToDfMap;
        this.docIdToLengthMap = docIdToLengthMap;
        this.isDocLengthInitialized = false;
    }

    public PostingIndex(RecordManager recman) throws IOException {
        this(new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_TITLEID),
                new SafeHTree<Integer, List<Posting>>(recman, TITLEID_TO_POSTINGS),
                new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_WORDSID),
                new SafeHTree<Integer, List<Posting>>(recman, WORDSID_TO_POSTINGS),
                new SafeBTree<Integer, Integer>(recman, DOCID_TO_TFMAX, Comparator.naturalOrder()),
                new SafeHTree<Integer, Integer>(recman, WORDID_TO_DF),
                new SafeHTree<Integer, Double>(recman, DOCID_TO_LENGTH));
    }

    private void updateDF(Integer wordId) throws IOException {
        final var docIds = new HashSet<Integer>();

        final var titlePostings = titleIdToPostingsMap.get(wordId);
        if (titlePostings != null)
            docIds.addAll(titlePostings.stream().map(Posting::docId).toList());

        final var wordPostings = wordsIdToPostingsMap.get(wordId);
        if (wordPostings != null)
            docIds.addAll(wordPostings.stream().map(Posting::docId).toList());

        final var df = docIds.size();
        wordIdToDfMap.put(wordId, df);
    }

    private void updateTFMax(Integer docId, Integer wordId) throws IOException {
        final var posting = new Posting(docId);

        var titleCount = 0;
        final var titlePostings = titleIdToPostingsMap.get(wordId);
        if (titlePostings != null) {
            final var titleIdx = Collections.binarySearch(titlePostings, posting, Comparator.comparing(Posting::docId));
            if (0 <= titleIdx && titleIdx < titlePostings.size())
                titleCount = titlePostings.get(titleIdx).locations().size();
        }

        var wordCount = 0;
        final var wordPostings = wordsIdToPostingsMap.get(wordId);
        if (wordPostings != null) {
            final var wordIdx = Collections.binarySearch(wordPostings, posting, Comparator.comparing(Posting::docId));
            if (0 <= wordIdx && wordIdx < wordPostings.size())
                wordCount = wordPostings.get(wordIdx).locations().size();
        }

        final var tfMax = titleCount + wordCount;
        final var curTFMax = docIdToTFMaxMap.find(docId);
        if (curTFMax == null || curTFMax < tfMax)
            docIdToTFMaxMap.insert(docId, tfMax);
    }

    private void addWordToIndex(Integer docId, Integer wordId, Integer location,
            SafeHTree<Integer, Set<Integer>> forwardIndexMap,
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

        // Update the TFMax and DF
        updateTFMax(docId, wordId);
        updateDF(wordId);

        // Set the document length initialization flag to false
        isDocLengthInitialized = false;
    }

    /**
     * Adds a title to the posting index for a given document.
     *
     * @param docId    The ID of the document
     * @param titleId  The ID of the title
     * @param location The location of the title
     * @throws IndexerException if an error occurs while adding the title to the
     *                          index.
     */
    public void addTitle(Integer docId, Integer titleId, Integer location) {
        try {
            addWordToIndex(docId, titleId, location, docIdToTitleIdMap, titleIdToPostingsMap);
        } catch (IOException e) {
            throw new IndexerException(String.format("DocId: %d", docId), e);
        }
    }

    /**
     * Adds a word to the posting index for a given document.
     *
     * @param docId    the ID of the document
     * @param wordId   the ID of the word
     * @param location the location of the word
     * @throws IndexerException if an error occurs while adding the word to the
     *                          index
     */
    public void addWord(Integer docId, Integer wordId, Integer location) {
        try {
            addWordToIndex(docId, wordId, location, docIdToWordsIdMap, wordsIdToPostingsMap);
        } catch (IOException e) {
            throw new IndexerException(String.format("DocId: %d", docId), e);
        }
    }

    private void removeDocumentFromIndex(Integer docId, SafeHTree<Integer, Set<Integer>> forwardIndexMap,
            SafeHTree<Integer, List<Posting>> invertedIndexMap) throws IOException {
        final var forwardIndex = forwardIndexMap.get(docId);
        if (forwardIndex == null)
            return;
        forwardIndexMap.remove(docId);
        docIdToTFMaxMap.remove(docId);

        for (final var wordId : forwardIndex) {
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

            updateDF(wordId);
        }

        // Set the document length initialization flag to false
        isDocLengthInitialized = false;
    }

    /**
     * Removes a document from the posting index.
     *
     * @param docId the ID of the document to be removed
     * @throws IndexerException if an error occurs while removing the document
     */
    public void removeDocument(Integer docId) {
        try {
            removeDocumentFromIndex(docId, docIdToTitleIdMap, titleIdToPostingsMap);
            removeDocumentFromIndex(docId, docIdToWordsIdMap, wordsIdToPostingsMap);
        } catch (IOException e) {
            throw new IndexerException(String.format("DocId: %d", docId), e);
        }
    }

    private void initializeDocumentLength() {
        // No need to reinitialize if already initialized
        if (isDocLengthInitialized)
            return;

        try {
            // Clear the existing document lengths.
            // We do not use recman.delete() because deleting and creating a record
            // with the same name can cause weird issues
            for (final var key : docIdToLengthMap.keySet()) {
                docIdToLengthMap.remove(key);
            }

            // Calculate the document lengths by iterating over the inverted index
            // and adding (tf * idf / tfMax)^2 for each term in the index
            final var indexSize = docIdToTFMaxMap.size();

            for (final var postings : wordsIdToPostingsMap) {
                final var wordId = postings.getKey();
                final var df = wordIdToDfMap.get(wordId);
                final var idf = Math.log((double) indexSize / df);

                for (final var posting : postings.getValue()) {
                    final var docId = posting.docId();
                    final var tf = posting.locations().size();
                    final var tfMax = docIdToTFMaxMap.find(docId);

                    final var toAdd = Math.pow(tf * idf / tfMax, 2.0);

                    var curScore = docIdToLengthMap.get(docId);
                    if (curScore == null)
                        curScore = 0.0;
                    docIdToLengthMap.put(docId, curScore + toAdd);
                }
            }

            for (final var postings : titleIdToPostingsMap) {
                final var titleId = postings.getKey();
                final var df = wordIdToDfMap.get(titleId);
                final var idf = Math.log((double) indexSize / df);

                for (final var posting : postings.getValue()) {
                    final var docId = posting.docId();
                    final var tf = posting.locations().size();
                    final var tfMax = docIdToTFMaxMap.find(docId);

                    final var toAdd = Math.pow(tf * idf / tfMax, 2.0);

                    var curScore = docIdToLengthMap.get(docId);
                    if (curScore == null)
                        curScore = 0.0;
                    docIdToLengthMap.put(docId, curScore + toAdd);
                }
            }

            isDocLengthInitialized = true;
        } catch (IOException e) {
            throw new IndexerException("Error while initializing document lengths", e);
        }
    }

    private Map<Integer, Double> getScoresForIndex(Set<Integer> wordIds,
            SafeHTree<Integer, List<Posting>> invertedIndexMap)
            throws IOException {
        // Initialize the document lengths
        initializeDocumentLength();

        final var indexSize = docIdToTFMaxMap.size();
        final var scores = new HashMap<Integer, Double>();
        for (final var wordId : wordIds) {
            final var df = wordIdToDfMap.get(wordId);
            final var idf = Math.log((double) indexSize / df);

            final var postings = invertedIndexMap.get(wordId);
            if (postings == null)
                continue;

            for (final var posting : postings) {
                final var docId = posting.docId();
                final var tf = posting.locations().size();
                final var tfMax = docIdToTFMaxMap.find(docId);
                final var docLength = Math.sqrt(docIdToLengthMap.get(docId));

                final var score = tf * idf / tfMax / docLength;

                scores.merge(docId, score, Double::sum);
            }
        }

        return scores;
    }

    /**
     * Calculates the scores for a given set of word IDs. Currently, match in title
     * are given more weight by multiplying the score by 2.
     *
     * @param wordIds the set of word IDs for which scores need to be calculated
     * @return a map of document IDs to their corresponding scores
     * @throws IndexerException if an error occurs while calculating the scores
     */
    public Map<Integer, Double> getScores(Set<Integer> wordIds) {
        try {
            // Individual scores for title and body
            final var titleScores = getScoresForIndex(wordIds, titleIdToPostingsMap);
            final var bodyScores = getScoresForIndex(wordIds, wordsIdToPostingsMap);

            // Double the score for title to make it more important
            titleScores.replaceAll((k, v) -> 2 * v);

            // Combine the scores
            final var totalScores = Stream.of(titleScores, bodyScores).flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, Double::sum));

            return totalScores;

        } catch (IOException e) {
            throw new IndexerException("Error while calculating scores", e);
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

        System.out.println("DOCID_TO_TFMAX:");
        for (final var entry : docIdToTFMaxMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("WORDID_TO_DF:");
        for (final var entry : wordIdToDfMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("DOCID_TO_LENGTH:");
        for (final var entry : docIdToLengthMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }
}
