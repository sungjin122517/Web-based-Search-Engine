package com.comp4321.indexers;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.util.ParserException;

import com.comp4321.Crawler;
import com.comp4321.StopStem;
import com.comp4321.IRUtilities.Porter;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class Indexer implements AutoCloseable {
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

    private boolean isFreshDocument(String url) throws ParserException {
        final var crawler = new Crawler(url);
        final var curLastModified = crawler.getLastModified();

        final var docId = urlIndexer.getOrCreateDocumentId(url);
        final var metadata = metadataIndexer.getMetadata(docId);

        return metadata == null || curLastModified.isAfter(metadata.lastModified());
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
        final var links = crawler.extractLinks().stream().map(urlIndexer::getOrCreateDocumentId)
                .collect(Collectors.toSet());
        linkIndexer.addLinks(docId, links);

        // Add title and words to word index
        final var titles = crawler.extractTitle(true)
                .stream()
                .map(String::toLowerCase)
                .filter(w -> !stopStem.isStopWord(w))
                .map(porter::stripAffixes)
                .filter(w -> !w.isBlank())
                .map(wordIndexer::getOrCreateId)
                .toList();
        IntStream.range(0, titles.size()).forEach(i -> {
            try {
                invertedIndex.addTitle(docId, titles.get(i), i);
            } catch (IOException e) {
                throw new IndexerException("Failed to add title to inverted index", e);
            }
        });

        final var words = crawler.extractWords()
                .stream()
                .map(String::toLowerCase)
                .filter(w -> !stopStem.isStopWord(w))
                .map(porter::stripAffixes)
                .filter(w -> !w.isBlank())
                .map(wordIndexer::getOrCreateId)
                .toList();
        IntStream.range(0, words.size()).forEach(i -> {
            try {
                invertedIndex.addWord(docId, words.get(i), i);
            } catch (IOException e) {
                throw new IndexerException("Failed to add word to inverted index", e);
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

    /**
     * Searches for documents for the set of words.
     *
     * @param words the set of words to search for
     * @return a map of document urls to their corresponding scores
     */
    public Map<String, Double> search(Set<String> words) throws IOException {
        final var wordIds = words.stream().map(String::toLowerCase)
                .filter(w -> !stopStem.isStopWord(w))
                .map(porter::stripAffixes)
                .filter(w -> !w.isBlank())
                .map(wordIndexer::getOrCreateId)
                .collect(Collectors.toSet());
        final var scores = invertedIndex.getScores(wordIds);

        final var urlScores = scores.entrySet().stream()
                .collect(Collectors.toMap(e -> urlIndexer.getURL(e.getKey()), Map.Entry::getValue));

        return urlScores;
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
