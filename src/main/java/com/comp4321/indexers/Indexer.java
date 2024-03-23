package com.comp4321.indexers;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

            // Add the url and get the metadata
            final var docId = urlIndexer.getOrCreateDocumentId(url);
            final var metadata = metadataIndexer.getMetadata(docId);

            // Skip if the document is already indexed and not modified
            if (metadata != null && !curLastModified.isAfter(metadata.lastModified()))
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
            e.printStackTrace();
        }
    }

    public void printAll() throws IOException {
        urlIndexer.printAll();
        metadataIndexer.printAll();
        linkIndexer.printAll();
        wordIndexer.printAll();
        postingIndex.printAll();
    }

    @Override
    public void close() throws IOException {
        recman.commit();
        recman.close();
    }
}
