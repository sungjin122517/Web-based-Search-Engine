package com.comp4321.indexers;

import java.io.File;
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

    // method to output a plain-text file named spider_result.txt
    public void outputSpiderResult(String filename) throws IOException {
        var metadataMap = metadataIndexer.getMetadataMap();

        // create string with page title /n url /n last modification date, size of page /n list of keyword: frequency /n child links
        var content = new StringBuilder();
        for (final var entry : metadataMap) {
            var docId = entry.getKey();
            var metadata = entry.getValue();
            var title = metadata.title();
            var lastModified = metadata.lastModified();
            var pageSize = metadata.pageSize();
            var wordsId = postingIndex.getWordsId(docId);
            var linksId = linkIndexer.getChildLinksId(docId);

            content.append(title).append("\n");
            content.append(urlIndexer.getURL(docId)).append("\n");
            content.append(lastModified).append(", ").append(pageSize).append("\n");

            int count = 0;
            for (var wordId : wordsId) {
                var word = wordIndexer.getWord(wordId);
                var frequency = postingIndex.getWordFrequency(wordId, docId);
                content.append(word).append(" ").append(frequency).append("; ");
                if (++count >= 10)
                    break;
            }
            content.append("\n");

            count = 0;
            for (var linkId : linksId) {
                var childUrl = urlIndexer.getURL(linkId);
                content.append(childUrl).append("\n");
                if (++count >= 10)
                    break;
            }
            content.append("--------------------").append("\n");
        }

        // create file, write content to file, and save file in the root directory of this project
        var file = new File(filename);
        var writer = new java.io.FileWriter(file);
        writer.write(content.toString());
        writer.close();

        System.out.println("Output spider result to spider_result.txt");
    }

    @Override
    public void close() throws IOException {
        recman.commit();
        recman.close();
    }
}
