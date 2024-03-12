package com.comp4321.indexers;

import java.io.IOException;

import org.htmlparser.util.ParserException;

import com.comp4321.Crawler;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import jdbm.btree.BTree;
import jdbm.helper.StringComparator;
import jdbm.htree.HTree;

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
        urlIndexer = new URLIndexer(getBTree(URL_MAP), maxPages);
        linkIndexer = new LinkIndexer(getHTree(PARENT_TO_CHILD), getHTree(CHILD_TO_PARENT), maxPages);
        metadataIndexer = new MetadataIndexer(getHTree(METADATA_MAP), maxPages);
    }

    private BTree getBTree(String name) throws IOException {
        long recid = recman.getNamedObject(name);
        if (recid != 0) {
            return BTree.load(recman, recid);
        } else {
            final var btree = BTree.createInstance(recman, new StringComparator());
            recman.setNamedObject(name, btree.getRecid());
            return btree;
        }
    }

    private HTree getHTree(String name) throws IOException {
        long recid = recman.getNamedObject(name);
        if (recid != 0) {
            return HTree.load(recman, recid);
        } else {
            final var htree = HTree.createInstance(recman);
            recman.setNamedObject(name, htree.getRecid());
            return htree;
        }
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

            // Add the links to PARENT_TO_CHILD and CHILD_TO_PARENT
            crawler.extractLinks().stream()
                    .map(urlIndexer::getOrCreateDocumentId)
                    .forEach(childId -> {
                        linkIndexer.addLink(docId, childId);
                    });

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
