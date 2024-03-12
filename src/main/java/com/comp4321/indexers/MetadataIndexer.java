package com.comp4321.indexers;

import java.io.IOException;

import jdbm.htree.HTree;

public class MetadataIndexer {
    private final HTree metadataMap;
    private final int maxPages;

    public MetadataIndexer(HTree metadataMap, int maxPages) {
        this.metadataMap = metadataMap;
        this.maxPages = maxPages;
    }

    public Metadata getMetadata(int docId) {
        try {
            return (Metadata) metadataMap.get(docId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void setMetadata(int docId, Metadata metadata) {
        if (docId > maxPages)
            return;

        try {
            metadataMap.put(docId, metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("METADATA_MAP:");
        final var docIds = metadataMap.keys();
        var docId = (Integer) docIds.next();
        while (docId != null) {
            final var metadata = (Metadata) metadataMap.get(docId);
            System.out.println(docId + " -> " + metadata.toString());
            docId = (Integer) docIds.next();
        }
        System.out.println();
    }
}
