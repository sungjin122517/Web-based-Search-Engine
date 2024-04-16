package com.comp4321.indexers;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.util.ParserException;

import com.comp4321.IRUtilities.Crawler;
import com.comp4321.SearchEngine;
import com.comp4321.SearchResult;
import com.comp4321.IRUtilities.Porter;
import com.comp4321.IRUtilities.StopStem;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class Indexer implements AutoCloseable, SearchEngine {
    private static final String DB_NAME = "indexes";

    private final RecordManager recman;
    private final URLIndexer urlIndexer;
    private final MetadataIndexer metadataIndexer;
    private final LinkIndexer linkIndexer;
    private final WordIndexer wordIndexer;
    private final InvertedIndex invertedIndex;

    private final StopStem stopStem = new StopStem();
    private final Porter porter = new Porter();

    public Indexer() throws IOException {
        recman = RecordManagerFactory.createRecordManager(DB_NAME);
        urlIndexer = new URLIndexer(recman);
        linkIndexer = new LinkIndexer(recman);
        metadataIndexer = new MetadataIndexer(recman);
        wordIndexer = new WordIndexer(recman);
        invertedIndex = new InvertedIndex(recman);
    }

    private boolean isFreshDocument(String url) throws IOException {
        final var crawler = new Crawler(url);
        final var curLastModified = crawler.getLastModified();

        final var docId = urlIndexer.getOrCreateDocumentId(url);
        final var metadata = metadataIndexer.getMetadata(docId);

        return metadata == null || curLastModified.isAfter(metadata.lastModified());
    }

    /**
        * Stems a word by converting it to lowercase, removing any affixes using Porter's algorithm,
        * and checking if it is a stop word.
        *
        * @param word the word to be stemmed
        * @return an Optional containing the stemmed word if it is not a stop word and not blank,
        *         or an empty Optional otherwise
        */
    public Optional<String> stemWord(String word) {
        word = word.toLowerCase();
        if (stopStem.isStopWord(word))
            return Optional.empty();

        word = porter.stripAffixes(word);
        if (word.isBlank())
            return Optional.empty();

        return Optional.of(word);
    }

    /**
     * Indexes a document by crawling the given URL, extracting metadata, links,
     * title, and words, and adding them to the respective indexes.
     *
     * @param url the URL of the document to be indexed
     */
    public void indexDocument(String url) throws IOException, ParserException {
        final var crawler = new Crawler(url);
        final var curLastModified = crawler.getLastModified();

        // Skip if the document is already indexed and not modified
        final var docId = urlIndexer.getOrCreateDocumentId(url);
        if (!isFreshDocument(url))
            return;

        // Remove the old postings when the document is modified
        // Other indexes automatically overwrite the old data
        invertedIndex.removeDocument(docId);

        // Add the metadata to metadata index
        final var title = String.join(" ", crawler.extractTitle(false));
        final var pageSize = crawler.getPageSize();
        metadataIndexer.addMetadata(docId, new Metadata(title, curLastModified, pageSize));

        // Add the links to link index
        final var links = crawler.extractLinks().stream().map(childUrl -> {
            try {
                return urlIndexer.getOrCreateDocumentId(childUrl);
            } catch (IOException e) {
                throw new IndexerException("Failed to get or create document ID", e);
            }
        })
                .collect(Collectors.toSet());
        linkIndexer.addLinks(docId, links);

        // Add title and words to word index
        final var titles = crawler.extractTitle(true)
                .stream()
                .map(this::stemWord)
                .flatMap(Optional::stream)
                .map(word -> {
                    try {
                        return wordIndexer.getOrCreateId(word);
                    } catch (IOException e) {
                        throw new IndexerException("Failed to get or create word ID for title", e);
                    }
                })
                .toList();
        IntStream.range(0, titles.size()).forEach(i -> {
            try {
                invertedIndex.addTitle(docId, titles.get(i), i);
            } catch (IOException e) {
                throw new IndexerException("Failed to add title to inverted title index", e);
            }
        });

        final var words = crawler.extractWords()
                .stream()
                .map(this::stemWord)
                .flatMap(Optional::stream)
                .map(word -> {
                    try {
                        return wordIndexer.getOrCreateId(word);
                    } catch (IOException e) {
                        throw new IndexerException("Failed to get or create word ID for body", e);
                    }
                })
                .toList();
        IntStream.range(0, words.size()).forEach(i -> {
            try {
                invertedIndex.addWord(docId, words.get(i), i);
            } catch (IOException e) {
                throw new IndexerException("Failed to add word to inverted body index", e);
            }
        });
    }

    /**
     * Performs a breadth-first search starting from the specified base URL and
     * visits a maximum number of pages.
     * 
     * @param baseURL  The base URL to start the search from.
     * @param maxPages The maximum number of pages to visit.
     */
    public void bfs(String baseURL, int maxPages) throws IOException, ParserException {
        final var queue = new ArrayDeque<String>();
        final var visited = new HashSet<String>();

        try (final var pb = new ProgressBarBuilder()
                .setTaskName("Crawl")
                .setInitialMax(maxPages)
                .setStyle(ProgressBarStyle.ASCII)
                .build()) {
            visited.add(baseURL);
            if (isFreshDocument(baseURL)) {
                queue.add(baseURL);
                indexDocument(baseURL);
            }
            pb.step();

            final var lb = new LinkBean();
            while (!queue.isEmpty() && visited.size() < maxPages) {
                final var curURL = queue.remove();
                lb.setURL(curURL);

                Arrays.stream(lb.getLinks())
                        .map(URL::toString)
                        .forEach(link -> {
                            try {
                                if (!visited.contains(link) && visited.size() < maxPages) {
                                    visited.add(link);
                                    if (isFreshDocument(link)) {
                                        queue.add(link);
                                        indexDocument(link);
                                    }
                                    pb.step();
                                }
                            } catch (IOException | ParserException e) {
                                throw new IndexerException("Failed to index document", e);
                            }
                        });
            }
        }
    }

    private SearchResult buildSearchResult(Integer docId, Double score) throws IOException {
        final var metadata = metadataIndexer.getMetadata(docId);
        final var url = urlIndexer.getURL(docId);

        final var keywordFrequencies = new HashMap<String, Integer>();
        invertedIndex.getKeywordsWithFrequency(docId).forEach((wordId, freq) -> {
            try {
                final var word = wordIndexer.getWord(wordId);
                keywordFrequencies.put(word, freq);
            } catch (IOException e) {
                throw new IndexerException("Failed to get word", e);
            }
        });

        final var parentLinks = linkIndexer.getParentLinks(docId).stream().map(parentId -> {
            try {
                return urlIndexer.getURL(parentId);
            } catch (IOException e) {
                throw new IndexerException("Failed to get URL", e);
            }
        }).collect(Collectors.toSet());

        final var childLinks = linkIndexer.getChildLinks(docId).stream().map(childId -> {
            try {
                return urlIndexer.getURL(childId);
            } catch (IOException e) {
                throw new IndexerException("Failed to get URL", e);
            }
        }).collect(Collectors.toSet());

        return new SearchResult(score, metadata.title(), url, metadata.lastModified(),
                metadata.pageSize(),
                keywordFrequencies, parentLinks, childLinks);
    }

    /**
     * Searches for the given set of words and phrase in the index.
     * Returns a map of docIds and their corresponding search results.
     *
     * @param words  the set of words to search for (words in thephrase are
     *               included)
     * @param phrase the phrase to search for (if any)
     * @return a map of docIds and their corresponding search results
     * @throws IOException if an I/O error occurs while searching the index
     */
    @Override
    public Map<Integer, SearchResult> search(Set<String> words, List<String> phrase) throws IOException {
        // Compute the scores for the given words
        final var wordIds = words.stream()
                .map(this::stemWord)
                .flatMap(Optional::stream)
                .map(word -> {
                    try {
                        return wordIndexer.getOrCreateId(word);
                    } catch (IOException e) {
                        throw new IndexerException("Failed to get or create word ID for query", e);
                    }
                })
                .collect(Collectors.toSet());
        final var scores = invertedIndex.getScores(wordIds);

        // Get the documents with the given phrase
        final var phraseIds = phrase.stream()
                .map(this::stemWord)
                .flatMap(Optional::stream)
                .map(word -> {
                    try {
                        return wordIndexer.getOrCreateId(word);
                    } catch (IOException e) {
                        throw new IndexerException("Failed to get or create word ID for phrase", e);
                    }
                }).toList();
        final var documentsWithPhrase = invertedIndex.getDocumentsWithPhrase(phraseIds);

        // Filter the scores with the documents with the given phrase
        // and convert to the search result
        return scores.entrySet().stream()
                .filter(entry -> documentsWithPhrase.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
                    try {
                        return buildSearchResult(entry.getKey(), entry.getValue());
                    } catch (IOException e) {
                        throw new IndexerException("Failed to build search result", e);
                    }
                }));
    }

    /**
     * Prints all the indexes.
     *
     * @throws IOException if an I/O error occurs.
     */
    public void printAll() throws IOException {
        urlIndexer.printAll();
        metadataIndexer.printAll();
        linkIndexer.printAll();
        wordIndexer.printAll();
        invertedIndex.printAll();
    }

    @Override
    public void close() throws IOException {
        recman.commit();
        recman.close();
    }
}
