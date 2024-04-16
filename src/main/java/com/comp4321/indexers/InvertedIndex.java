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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.comp4321.indexers.posting.Posting;
import com.comp4321.indexers.posting.PostingIndex;
import com.comp4321.jdbm.SafeBTree;
import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class InvertedIndex {
    public static final String DOCID_TO_TITLEID = "docIdToTitleId";
    public static final String TITLEID_TO_POSTINGS = "titleIdToPostings";
    public static final String DOCID_TO_WORDSID = "docIdToWordsId";
    public static final String WORDSID_TO_POSTINGS = "wordsIdToPostings";
    public static final String DOCID_TO_TFMAX = "docIdToTfMax";
    public static final String WORDID_TO_DF = "wordIdToDf";

    private final PostingIndex titleIndex;
    private final PostingIndex bodyIndex;
    private final SafeBTree<Integer, Integer> docIdToTFMaxMap;
    private final SafeBTree<Integer, Integer> wordIdToDFMap;

    public InvertedIndex(PostingIndex titleIndex, PostingIndex bodyIndex,
            SafeBTree<Integer, Integer> docIdToTFMaxMap,
            SafeBTree<Integer, Integer> wordIdToDFMap) {
        this.titleIndex = titleIndex;
        this.bodyIndex = bodyIndex;
        this.docIdToTFMaxMap = docIdToTFMaxMap;
        this.wordIdToDFMap = wordIdToDFMap;
    }

    public InvertedIndex(RecordManager recman) throws IOException {
        this(new PostingIndex("Title", new SafeHTree<>(recman, DOCID_TO_TITLEID),
                new SafeHTree<>(recman, TITLEID_TO_POSTINGS)),
                new PostingIndex("Body", new SafeHTree<>(recman, DOCID_TO_WORDSID),
                        new SafeHTree<>(recman, WORDSID_TO_POSTINGS)),
                new SafeBTree<>(recman, DOCID_TO_TFMAX, Comparator.<Integer>naturalOrder()),
                new SafeBTree<>(recman, WORDID_TO_DF, Comparator.<Integer>naturalOrder()));
    }

    private void updateDF(Integer wordId) throws IOException {
        final var docIds = new HashSet<Integer>();

        final var titlePostings = titleIndex.getPostings(wordId);
        if (titlePostings != null)
            docIds.addAll(titlePostings.stream().map(Posting::docId).toList());

        final var wordPostings = bodyIndex.getPostings(wordId);
        if (wordPostings != null)
            docIds.addAll(wordPostings.stream().map(Posting::docId).toList());

        final var df = docIds.size();
        wordIdToDFMap.insert(wordId, df);
    }

    private void updateTFMax(Integer docId, Integer wordId) throws IOException {
        final var posting = new Posting(docId);

        var titleCount = 0;
        final var titlePostings = titleIndex.getPostings(wordId);
        if (titlePostings != null) {
            final var titleIdx = Collections.binarySearch(titlePostings, posting, Comparator.comparing(Posting::docId));
            if (0 <= titleIdx && titleIdx < titlePostings.size())
                titleCount = titlePostings.get(titleIdx).locations().size();
        }

        var wordCount = 0;
        final var wordPostings = bodyIndex.getPostings(wordId);
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

    /**
     * Adds a title to the posting index for a given document.
     *
     * @param docId    The ID of the document
     * @param titleId  The ID of the title
     * @param location The location of the title
     * @throws IOException if an error occurs while adding the title to the
     *                     index.
     */
    public void addTitle(Integer docId, Integer titleId, Integer location) throws IOException {
        titleIndex.addWord(docId, titleId, location);

        // Update the TFMax and DF
        updateTFMax(docId, titleId);
        updateDF(titleId);
    }

    /**
     * Adds a word to the posting index for a given document.
     *
     * @param docId    the ID of the document
     * @param wordId   the ID of the word
     * @param location the location of the word
     * @throws IOException if an error occurs while adding the word to the
     *                     index
     */
    public void addWord(Integer docId, Integer wordId, Integer location) throws IOException {
        bodyIndex.addWord(docId, wordId, location);

        // Update the TFMax and DF
        updateTFMax(docId, wordId);
        updateDF(wordId);
    }

    /**
     * Removes a document from the posting index.
     *
     * @param docId the ID of the document to be removed
     * @throws IOException if an error occurs while removing the document
     */
    public void removeDocument(Integer docId) throws IOException {
        // Retrieve all the word IDs for the document
        final var titleWordIds = titleIndex.getForwardWords(docId);
        final var bodyWordIds = bodyIndex.getForwardWords(docId);
        final var totalWordIds = Stream.of(titleWordIds, bodyWordIds).flatMap(m -> {
            if (m == null)
                return Stream.empty();
            return m.stream();
        }).distinct();

        // Remove the document from the forward index
        titleIndex.removeDocument(docId);
        bodyIndex.removeDocument(docId);

        // Update the TFMax and DF
        docIdToTFMaxMap.remove(docId);
        totalWordIds.forEach(wordId -> {
            try {
                // It is safe to just decrement the DF by 1 since the document is being removed
                final var df = wordIdToDFMap.find(wordId) - 1;
                wordIdToDFMap.insert(wordId, df);
            } catch (IOException e) {
                throw new IndexerException("Error while updating DF", e);
            }
        });
    }

    private Double getDocumentLength(Integer docId) throws IOException {
        // Calculate the document lengths by iterating over the inverted index
        // and adding (tf * idf / tfMax)^2 for each term in the index
        final var totalDocuments = docIdToTFMaxMap.size();
        final var tfMax = docIdToTFMaxMap.find(docId);

        final var titleWords = titleIndex.getForwardWords(docId);
        final var bodyWords = bodyIndex.getForwardWords(docId);
        final var totalWords = Stream.of(titleWords, bodyWords).flatMap(m -> m.stream()).distinct();

        final var docLen = totalWords.mapToDouble(wordId -> {
            try {
                var titleTF = 0;
                final var titlePostings = titleIndex.getPostings(wordId);
                if (titlePostings != null) {
                    final var titleIdx = Collections.binarySearch(titlePostings, new Posting(docId),
                            Comparator.comparing(Posting::docId));
                    if (0 <= titleIdx && titleIdx < titlePostings.size())
                        titleTF = titlePostings.get(titleIdx).locations().size();
                }

                var bodyTF = 0;
                final var bodyPostings = bodyIndex.getPostings(wordId);
                if (bodyPostings != null) {
                    final var bodyIdx = Collections.binarySearch(bodyPostings, new Posting(docId),
                            Comparator.comparing(Posting::docId));
                    if (0 <= bodyIdx && bodyIdx < bodyPostings.size())
                        bodyTF = bodyPostings.get(bodyIdx).locations().size();
                }

                final var tf = titleTF + bodyTF;
                final var df = wordIdToDFMap.find(wordId);
                final var idf = Math.log((double) totalDocuments / df);
                return Math.pow(tf * idf / tfMax, 2.0);
            } catch (IOException e) {
                throw new IndexerException("Error while calculating document length", e);
            }
        }).sum();

        return Math.sqrt(docLen);
    }

    private Map<Integer, Double> computeScoresForWord(Integer wordId) throws IOException {
        // Scores are calculated as (TITLE_MATCH_MULTIPLIER * title_tf + body_tf) *
        // log(N / df) / tfMax
        // title_tf: term frequency in the title of the document
        // body_tf: term frequency in the body of the document
        // N: total number of documents
        // df: document frequency of the term in either title or body
        // tfMax: maximum (title_df + body_tf) in the document
        final var scores = new HashMap<Integer, Double>();

        final var TITLE_MATCH_MULTIPLIER = 10;
        final var totalDocuments = docIdToTFMaxMap.size();

        var titlePostings = titleIndex.getPostings(wordId);
        if (titlePostings == null)
            titlePostings = new ArrayList<>();

        var bodyPostings = bodyIndex.getPostings(wordId);
        if (bodyPostings == null)
            bodyPostings = new ArrayList<>();

        var titleIdx = 0;
        var bodyIdx = 0;

        // Iterate over the title and body postings and calculate the scores
        while (titleIdx < titlePostings.size() && bodyIdx < bodyPostings.size()) {
            final var titlePosting = titlePostings.get(titleIdx);
            final var bodyPosting = bodyPostings.get(bodyIdx);

            final var titleDocId = titlePosting.docId();
            final var bodyDocId = bodyPosting.docId();

            final var docId = Math.min(titleDocId, bodyDocId);
            final var tfMax = docIdToTFMaxMap.find(docId);
            final var df = wordIdToDFMap.find(wordId);
            final var idf = Math.log((double) totalDocuments / df);

            var tf = 0;
            if (titleDocId == docId) {
                tf += titlePosting.locations().size() * TITLE_MATCH_MULTIPLIER;
                ++titleIdx;
            }

            if (bodyDocId == docId) {
                tf += bodyPosting.locations().size();
                ++bodyIdx;
            }
            scores.merge(docId, tf * idf / tfMax, Double::sum);
        }

        while (titleIdx < titlePostings.size()) {
            final var titlePosting = titlePostings.get(titleIdx);
            final var docId = titlePosting.docId();
            final var tf = titlePosting.locations().size() * TITLE_MATCH_MULTIPLIER;
            final var tfMax = docIdToTFMaxMap.find(docId);
            final var df = wordIdToDFMap.find(wordId);
            final var idf = Math.log((double) totalDocuments / df);
            scores.merge(docId, tf * idf / tfMax, Double::sum);
            ++titleIdx;
        }

        while (bodyIdx < bodyPostings.size()) {
            final var bodyPosting = bodyPostings.get(bodyIdx);
            final var docId = bodyPosting.docId();
            final var tf = bodyPosting.locations().size();
            final var tfMax = docIdToTFMaxMap.find(docId);
            final var df = wordIdToDFMap.find(wordId);
            final var idf = Math.log((double) totalDocuments / df);
            scores.merge(docId, tf * idf / tfMax, Double::sum);
            ++bodyIdx;
        }

        return scores;
    }

    /**
     * Calculates the scores for a given set of word IDs.
     * Match in title is given priority by multiplying the term frequency in title
     * by 10 before calculating the score.
     * This means that scores are not bounded by 1.
     *
     * @param wordIds the set of word IDs for which scores need to be calculated
     * @return a map of document IDs to their corresponding scores
     * @throws IOException if an error occurs while calculating the scores
     */
    public Map<Integer, Double> getScores(Set<Integer> wordIds) throws IOException {
        final var scores = new HashMap<Integer, Double>();

        // Calculate the scores for each word and merge them into the final scores
        wordIds.stream().forEach(wordId -> {
            try {
                final var wordScores = computeScoresForWord(wordId);
                wordScores.forEach((docId, score) -> scores.merge(docId, score, Double::sum));
            } catch (IOException e) {
                throw new IndexerException("Error while getting title postings", e);
            }
        });

        // Normalize the scores by the document lengths
        scores.replaceAll((docId, score) -> {
            try {
                return score / getDocumentLength(docId);
            } catch (IOException e) {
                throw new IndexerException("Error while normalizing scores", e);
            }
        });

        return scores;
    }

    /**
     * Returns a set of document IDs that contain the given phrase.
     *
     * @param phrase a phrase represented as an ordered list of word IDs
     * @return a set of document IDs that contain the given phrase
     */
    public Set<Integer> getDocumentsWithPhrase(List<Integer> phrase) throws IOException {
        // Returns all the documents
        if (phrase.isEmpty()) {
            final var docIds = new HashSet<Integer>();
            for (final var entry : docIdToTFMaxMap)
                docIds.add(entry.getKey());
            return docIds;
        }

        final var titlePhrase = titleIndex.getDocumentsWithPhrase(phrase);
        final var bodyPhrase = bodyIndex.getDocumentsWithPhrase(phrase);

        titlePhrase.addAll(bodyPhrase);
        return titlePhrase;
    }

    /**
     * Retrieves the keywords with their corresponding frequencies for a given
     * document ID.
     *
     * @param docId The ID of the document.
     * @return A map containing the wordIds as keys and their frequencies as values.
     * @throws IOException If an I/O error occurs while retrieving the keywords and
     *                     frequencies.
     */
    public Map<Integer, Integer> getKeywordsWithFrequency(Integer docId) throws IOException {
        final var titleFrequencies = titleIndex.getForwardWords(docId)
                .stream().collect(Collectors.toMap(Function.identity(), wordId -> {
                    try {
                        return titleIndex.getPosting(docId, wordId).locations().size();
                    } catch (IOException e) {
                        throw new IndexerException("Error while getting title postings", e);
                    }
                }));
        final var bodyFrequencies = bodyIndex.getForwardWords(docId)
                .stream().collect(Collectors.toMap(Function.identity(), wordId -> {
                    try {
                        return bodyIndex.getPosting(docId, wordId).locations().size();
                    } catch (IOException e) {
                        throw new IndexerException("Error while getting body postings", e);
                    }
                }));

        return Stream.concat(titleFrequencies.entrySet().stream(), bodyFrequencies.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
    }

    public void printAll() {
        titleIndex.printAll();
        bodyIndex.printAll();

        System.out.println("DOCID_TO_TFMAX:");
        for (final var entry : docIdToTFMaxMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();

        System.out.println("WORDID_TO_DF:");
        for (final var entry : wordIdToDFMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }
}
