package com.comp4321.indexers;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.htmlparser.beans.LinkBean;
import org.htmlparser.util.ParserException;

import com.comp4321.Crawler;
import com.comp4321.StopStem;
import com.comp4321.IRUtilities.Porter;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

public class Indexer implements AutoCloseable {
    private static final String DB_NAME = "indexes";

    private final RecordManager recman;
    private final URLIndexer urlIndexer;
    private final MetadataIndexer metadataIndexer;
    private final LinkIndexer linkIndexer;
    private final WordIndexer wordIndexer;
    private final PostingIndex postingIndex;

    private final StopStem stopStem = new StopStem();
    private final Porter porter = new Porter();

    public Indexer() throws IOException {
        recman = RecordManagerFactory.createRecordManager(DB_NAME);
        urlIndexer = new URLIndexer(recman);
        linkIndexer = new LinkIndexer(recman);
        metadataIndexer = new MetadataIndexer(recman);
        wordIndexer = new WordIndexer(recman);
        postingIndex = new PostingIndex(recman);
    }

    private boolean isFreshDocument(String url) {
        try {
            final var crawler = new Crawler(url);
            final var curLastModified = crawler.getLastModified();

            final var docId = urlIndexer.getOrCreateDocumentId(url);
            final var metadata = metadataIndexer.getMetadata(docId);

            return metadata == null || curLastModified.isAfter(metadata.lastModified());
        } catch (ParserException e) {
            throw new IndexerException(url, e);
        }
    }

    /**
     * Indexes a document by crawling the given URL, extracting metadata, links,
     * title, and words, and adding them to the respective indexes.
     *
     * @param url the URL of the document to be indexed
     */
    public void indexDocument(String url) {
        try {
            final var crawler = new Crawler(url);
            final var curLastModified = crawler.getLastModified();

            // Skip if the document is already indexed and not modified
            final var docId = urlIndexer.getOrCreateDocumentId(url);
            if (!isFreshDocument(url))
                return;

            // Remove the old postings when the document is modified
            // Other indexes automatically overwrite the old data
            postingIndex.removeDocument(docId);

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
            IntStream.range(0, titles.size()).forEach(i -> postingIndex.addTitle(docId, titles.get(i), i));

            final var words = crawler.extractWords()
                    .stream()
                    .map(String::toLowerCase)
                    .filter(w -> !stopStem.isStopWord(w))
                    .map(porter::stripAffixes)
                    .filter(w -> !w.isBlank())
                    .map(wordIndexer::getOrCreateId)
                    .toList();
            IntStream.range(0, words.size()).forEach(i -> postingIndex.addWord(docId, words.get(i), i));

        } catch (ParserException e) {
            throw new IndexerException(url, e);
        }
    }

    /**
     * Performs a breadth-first search starting from the specified base URL and
     * visits a maximum number of pages.
     * 
     * @param baseURL  The base URL to start the search from.
     * @param maxPages The maximum number of pages to visit.
     */
    public void bfs(String baseURL, int maxPages) {
        final var queue = new ArrayDeque<String>();
        final var visited = new HashSet<String>();

        visited.add(baseURL);
        if (isFreshDocument(baseURL)) {
            queue.add(baseURL);
            indexDocument(baseURL);
        }

        final var lb = new LinkBean();
        while (!queue.isEmpty() && visited.size() < maxPages) {
            final var curURL = queue.remove();
            lb.setURL(curURL);

            Arrays.stream(lb.getLinks())
                    .map(URL::toString)
                    .forEach(link -> {
                        if (!visited.contains(link) && visited.size() < maxPages) {
                            visited.add(link);
                            if (isFreshDocument(link)) {
                                queue.add(link);
                                indexDocument(link);
                            }
                        }
                    });
        }
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
        postingIndex.printAll();
    }

    /**
     * Outputs the spider result to a file.
     *
     * @param filename the name of the file to write the spider result to
     * @throws IOException if an I/O error occurs while writing the file
     */
    public void outputSpiderResult(String filename) throws IOException {
        /*
         * page title
         * url
         * last modification date, size of page
         * keyword 1 frequency 1; keyword 2 frequency 2; ...; keyword 10 frequency 10
         * child link 1
         * child link 2
         * ...
         * child link 10
         * --------------------
         */
        final var content = new StringBuilder();
        for (final var entry : metadataIndexer) {
            final var docId = entry.getKey();

            final var metadata = entry.getValue();
            final var title = metadata.title();
            final var lastModified = metadata.lastModified();
            final var pageSize = metadata.pageSize();

            content.append(title).append("\n");
            content.append(urlIndexer.getURL(docId)).append("\n");
            content.append(lastModified).append(", ").append(pageSize).append("\n");

            postingIndex.getTotalWordsId(docId).stream().limit(10).forEach(wordId -> {
                final var word = wordIndexer.getWord(wordId);
                final var frequency = postingIndex.getTotalWordFrequency(docId, wordId);
                content.append(word).append(" ").append(frequency).append("; ");
            });
            content.append("\n");

            linkIndexer.getChildLinksId(docId).stream().limit(10).forEach(childId -> {
                final var childUrl = urlIndexer.getURL(childId);
                content.append(childUrl).append("\n");
            });
            content.append("--------------------").append("\n");
        }

        // create and write to filename
        Files.writeString(Paths.get(filename), content.toString(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("Finished writing spider result to " + filename);
    }

    @Override
    public void close() throws IOException {
        recman.commit();
        recman.close();
    }
}
