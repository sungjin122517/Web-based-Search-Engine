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

    private void updateTFMax(Integer docId) throws IOException {
        final var wordIds = postingIndex.getForwardWords(docId);

        final var tfMax = wordIds.stream().mapToInt(wordId -> {
            try {
                final var posting = postingIndex.getPosting(docId, wordId);
                return posting.titleLocations().size() + posting.bodyLocations().size();
            } catch (IOException e) {
                throw new IndexerException("Error while updating TFMax", e);
            }
        }).max();

        if (tfMax.isEmpty())
            throw new IndexerException("Error while updating TFMax: no words found");

        docIdToTFMaxMap.insert(docId, tfMax.getAsInt());
    }

    /**
     * Adds a document to the inverted index.
     *
     * @param docId     the ID of the document to be added
     * @param titleIds  the list of term IDs in the document's title in order
     * @param bodyIds   the list of term IDs in the document's body in order
     * @throws IOException if an I/O error occurs while adding the document
     */
    public void addDocument(Integer docId, List<Integer> titleIds, List<Integer> bodyIds) throws IOException {
        postingIndex.addDocument(docId, titleIds, bodyIds);
        updateTFMax(docId);
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
