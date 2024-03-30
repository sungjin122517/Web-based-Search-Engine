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

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class PostingIndex {
    public final static String DOCID_TO_TITLEID = "docIdToTitleId";
    public final static String TITLEID_TO_POSTINGS = "titleIdToPostings";
    public final static String DOCID_TO_WORDSID = "docIdToWordsId";
    public final static String WORDSID_TO_POSTINGS = "wordsIdToPostings";
    public final static String DOCID_TO_TFMAX = "docIdToTfMax";
    public final static String WORDID_TO_DF = "wordIdToDf";

    // Inverted indexes are maintained in sorted order for efficient search
    private final SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap;
    private final SafeHTree<Integer, List<Posting>> titleIdToPostingsMap;
    private final SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap;
    private final SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap;
    private final SafeHTree<Integer, Integer> docIdToTFMaxMap;
    private final SafeHTree<Integer, Integer> wordIdToDfMap;

    public PostingIndex(SafeHTree<Integer, Set<Integer>> docIdToTitleIdMap,
            SafeHTree<Integer, List<Posting>> titleIdToPostingsMap,
            SafeHTree<Integer, Set<Integer>> docIdToWordsIdMap,
            SafeHTree<Integer, List<Posting>> wordsIdToPostingsMap,
            SafeHTree<Integer, Integer> docIdToTFMaxMap,
            SafeHTree<Integer, Integer> wordIdToDfMap) {
        this.titleIdToPostingsMap = titleIdToPostingsMap;
        this.docIdToTitleIdMap = docIdToTitleIdMap;
        this.wordsIdToPostingsMap = wordsIdToPostingsMap;
        this.docIdToWordsIdMap = docIdToWordsIdMap;
        this.docIdToTFMaxMap = docIdToTFMaxMap;
        this.wordIdToDfMap = wordIdToDfMap;
    }

    public PostingIndex(RecordManager recman) throws IOException {
        this(new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_TITLEID),
                new SafeHTree<Integer, List<Posting>>(recman, TITLEID_TO_POSTINGS),
                new SafeHTree<Integer, Set<Integer>>(recman, DOCID_TO_WORDSID),
                new SafeHTree<Integer, List<Posting>>(recman, WORDSID_TO_POSTINGS),
                new SafeHTree<Integer, Integer>(recman, DOCID_TO_TFMAX),
                new SafeHTree<Integer, Integer>(recman, WORDID_TO_DF));
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
        final var curTFMax = docIdToTFMaxMap.get(docId);
        if (curTFMax == null || curTFMax < tfMax)
            docIdToTFMaxMap.put(docId, tfMax);
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

    /**
     * Retrieves a set of all word IDs associated with a given document ID.
     *
     * @param docId The document ID for which to retrieve the word IDs.
     * @return A set of all word IDs associated with the given document ID.
     * @throws IndexerException If an error occurs while retrieving the word IDs.
     */
    public Set<Integer> getTotalWordsId(Integer docId) {
        try {
            final var title = docIdToTitleIdMap.get(docId);
            final var body = docIdToWordsIdMap.get(docId);
            title.addAll(body);
            return title;
        } catch (IOException e) {
            throw new IndexerException(String.format("DocId: %d", docId), e);
        }
    }

    private int getWordFrequencyForIndex(Integer docId, Integer wordId,
            SafeHTree<Integer, List<Posting>> invertedIndexMap)
            throws IOException {
        final var postings = invertedIndexMap.get(wordId);
        if (postings == null)
            return 0;

        final var postingIdx = Collections.binarySearch(postings, new Posting(docId),
                Comparator.comparing(Posting::docId));
        if (postingIdx < 0)
            return 0;

        return postings.get(postingIdx).locations().size();
    }

    /**
     * Returns the total frequency of a word in a document.
     *
     * @param docId  the ID of the document
     * @param wordId the ID of the word
     * @return the total frequency of the word in the document
     * @throws IndexerException if an error occurs while accessing the index
     */
    public int getTotalWordFrequency(Integer docId, Integer wordId) {
        try {
            final var titleCount = getWordFrequencyForIndex(docId, wordId, titleIdToPostingsMap);
            final var wordCount = getWordFrequencyForIndex(docId, wordId, wordsIdToPostingsMap);

            return titleCount + wordCount;
        } catch (IOException e) {
            throw new IndexerException(String.format("DocId: %d", docId), e);
        }
    }

    private Map<Integer, Double> getScoresForIndex(Set<Integer> wordIds,
            SafeHTree<Integer, List<Posting>> invertedIndexMap, Integer indexSize)
            throws IOException {
        final var scores = new HashMap<Integer, Double>();

        for (final var wordId : wordIds) {
            final var df = wordIdToDfMap.get(wordId);
            final var idf = Math.log((double) indexSize / df);

            final var postings = invertedIndexMap.get(wordId);
            for (final var posting : postings) {
                final var docId = posting.docId();
                final var tf = posting.locations().size();
                final var tfMax = docIdToTFMaxMap.get(docId);

                final var score = tf * idf / tfMax;
                scores.merge(docId, score, Double::sum);
            }
        }

        return scores;
    }

    /**
     * Calculates the scores for a given set of word IDs. Currently, match in title
     * and body are considered equally important.
     *
     * @param wordIds   the set of word IDs for which scores need to be calculated
     * @param indexSize the total number of documents in the index
     * @return a map of document IDs to their corresponding scores
     * @throws IndexerException if an error occurs while calculating the scores
     */
    public Map<Integer, Double> getScores(Set<Integer> wordIds, Integer indexSize) {
        try {
            // Individual scores for title and body
            final var titleScores = getScoresForIndex(wordIds, titleIdToPostingsMap, indexSize);
            final var bodyScores = getScoresForIndex(wordIds, wordsIdToPostingsMap, indexSize);

            // Combine the scores
            final var totalScores = Stream.of(titleScores, bodyScores).flatMap(m -> m.entrySet().stream())
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue, Double::sum));

            // TODO: Normalize the scores to cosine similarity
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
    }
}
