package com.comp4321.indexers;

import java.io.IOException;
import java.util.Comparator;
import java.util.stream.Collectors;

import org.htmlparser.util.ParserException;

import com.comp4321.Crawler;
import com.comp4321.jdbm.SafeBTree;
import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;

public class Indexer implements AutoCloseable {
    private static final String DB_NAME = "indexes";
    private static final String URL_MAP = "urlMap";
    private static final String PARENT_TO_CHILD = "parentToChild";
    private static final String CHILD_TO_PARENT = "childToParent";
    private static final String METADATA_MAP = "metadataMap";

    private final RecordManager recman;
    private final URLIndexer urlIndexer;
    private final LinkIndexer linkIndexer;
    private final MetadataIndexer metadataIndexer;

    public Indexer(int maxPages) throws IOException {
        recman = RecordManagerFactory.createRecordManager(DB_NAME);
        urlIndexer = new URLIndexer(new SafeBTree<String, Integer>(recman, URL_MAP, Comparator.naturalOrder()),
                maxPages);
        linkIndexer = new LinkIndexer(new SafeHTree<>(recman, PARENT_TO_CHILD),
                new SafeHTree<>(recman, CHILD_TO_PARENT), maxPages);
        metadataIndexer = new MetadataIndexer(new SafeHTree<>(recman, METADATA_MAP), maxPages);
    }

    public void indexDocument(String url) {
        try {
            final var crawler = new Crawler(url);
            final var curLastModified = crawler.getLastModified();

            // Add the url to URL_MAP and get the metadata
            final var docId = urlIndexer.getOrCreateDocumentId(url);
            final var metadata = metadataIndexer.getMetadata(docId);

            // Skip if the document is already indexed and not modified
            if (metadata != null && !curLastModified.isAfter(metadata.lastModified()))
                return;

            // Add the metadata to METADATA_MAP
            metadataIndexer.setMetadata(docId, new Metadata(curLastModified));

            // Set the links in PARENT_TO_CHILD and CHILD_TO_PARENT
            final var links = crawler.extractLinks().stream().map(urlIndexer::getOrCreateDocumentId)
                    .collect(Collectors.toSet());
            linkIndexer.setLinks(docId, links);

        } catch (ParserException e) {
            e.printStackTrace();
        }
    }

    public void printAll() throws IOException {
        urlIndexer.printAll();
        metadataIndexer.printAll();
        linkIndexer.printAll();
    }

    @Override
    public void close() throws IOException {
        recman.commit();
        recman.close();
    }
}
