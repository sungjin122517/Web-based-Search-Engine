package com.comp4321.indexers;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.comp4321.jdbm.SafeBTree;
import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class InvertedIndex {
    public static final String DOCID_TO_WORDID = "docIdToWordId";
    public static final String WORDID_TO_DOCID = "wordIdToDocId";
    public static final String DOCID_TO_TFMAX = "docIdToTfMax";

    private final PostingIndex postingIndex;
    private final SafeBTree<Integer, Integer> docIdToTFMaxMap;

    public InvertedIndex(PostingIndex postingIndex,
            SafeBTree<Integer, Integer> docIdToTFMaxMap) {
        this.postingIndex = postingIndex;
        this.docIdToTFMaxMap = docIdToTFMaxMap;
    }

    public InvertedIndex(RecordManager recman) throws IOException {
        this(new PostingIndex("Postings", new SafeHTree<>(recman, DOCID_TO_WORDID),
                new SafeHTree<>(recman, WORDID_TO_DOCID)),
                new SafeBTree<>(recman, DOCID_TO_TFMAX, Comparator.<Integer>naturalOrder()));
    }

    private void updateTFMax(Integer docId, Integer wordId) throws IOException {
        final var posting = postingIndex.getPosting(docId, wordId);
        final var tf = posting.titleLocations().size() + posting.bodyLocations().size();

        final var curTFMax = docIdToTFMaxMap.find(docId);
        if (curTFMax == null || curTFMax < tf)
            docIdToTFMaxMap.insert(docId, tf);
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
        postingIndex.addTitle(docId, titleId, location);
        updateTFMax(docId, titleId);
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
        postingIndex.addBody(docId, wordId, location);
        updateTFMax(docId, wordId);
    }

    /**
     * Removes a document from the posting index.
     *
     * @param docId the ID of the document to be removed
     * @throws IOException if an error occurs while removing the document
     */
    public void removeDocument(Integer docId) throws IOException {
        postingIndex.removeDocument(docId);
        docIdToTFMaxMap.remove(docId);
    }

    private Double getDocumentLength(Integer docId) throws IOException {
        // Calculate the document lengths by iterating over the inverted index
        // and adding (tf * idf / tfMax)^2 for each term in the index
        final var totalDocuments = docIdToTFMaxMap.size();
        final var tfMax = docIdToTFMaxMap.find(docId);

        final var docLen = postingIndex.getForwardWords(docId).stream().mapToDouble(wordId -> {
            try {
                final var posting = postingIndex.getPosting(docId, wordId);

                final var titleTF = posting.titleLocations().size();
                final var bodyTF = posting.bodyLocations().size();

                final var tf = titleTF + bodyTF;
                final var df = postingIndex.getDF(wordId);
                final var idf = Math.log((double) totalDocuments / df);
                return Math.pow(tf * idf / tfMax, 2.0);
            } catch (IOException e) {
                throw new IndexerException("Error while calculating document length", e);
            }
        }).sum();

        return Math.sqrt(docLen);
    }

    private Map<Integer, Double> computeScoresForWord(Integer wordId) throws IOException {
        /*
         * Scores are calculated as:
         * (a * title_tf + (1 - a) * body_tf) * log(N / df) / tfMax
         * 
         * a: magic constant to give priority to title matches (default: 0.9)
         * title_tf: term frequency in the title of the document
         * body_tf: term frequency in the body of the document
         * N: total number of documents
         * df: document frequency of the term in either title or body
         * tfMax: maximum (title_df + body_tf) in the document
         */

        final var TITLE_MATCH_MULTIPLIER = 0.9;
        final var totalDocuments = docIdToTFMaxMap.size();

        final var df = postingIndex.getDF(wordId);
        final var idf = Math.log((double) totalDocuments / df);
        return postingIndex.getPostings(wordId).stream().collect(Collectors.toMap(Posting::docId, posting -> {
            try {
                final var docId = posting.docId();
                final var tfMax = docIdToTFMaxMap.find(docId);

                final var titleTF = posting.titleLocations().size();
                final var bodyTF = posting.bodyLocations().size();
                final var adjustedTF = TITLE_MATCH_MULTIPLIER * titleTF + (1 - TITLE_MATCH_MULTIPLIER) * bodyTF;

                return adjustedTF * idf / tfMax;
            } catch (IOException e) {
                throw new IndexerException("Error while calculating scores", e);
            }
        }));
    }

    /**
     * Calculates the scores for a given set of word IDs.
     *
     * @param wordIds the set of word IDs for which scores need to be calculated
     * @return a map of document IDs to their corresponding scores
     * @throws IOException if an error occurs while calculating the scores
     */
    public Map<Integer, Double> getScores(Set<Integer> wordIds) throws IOException {
        // Calculate the scores for each word and merge them into the final scores
        final var scores = wordIds.stream().flatMap(wordId -> {
            try {
                return computeScoresForWord(wordId).entrySet().stream();
            } catch (IOException e) {
                throw new IndexerException("Error while calculating scores", e);
            }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Double::sum));

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
        return postingIndex.getDocumentsWithPhrase(phrase);
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
        return postingIndex.getForwardWords(docId).stream().collect(Collectors.toMap(
                Function.identity(),
                wordId -> {
                    try {
                        final var posting = postingIndex.getPosting(docId, wordId);
                        return posting.titleLocations().size() + posting.bodyLocations().size();
                    } catch (IOException e) {
                        throw new IndexerException("Error while getting keywords with frequency", e);
                    }
                }));
    }

    public void printAll() {
        postingIndex.printAll();

        System.out.println("DOCID_TO_TFMAX:");
        for (final var entry : docIdToTFMaxMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        System.out.println();
    }
}
