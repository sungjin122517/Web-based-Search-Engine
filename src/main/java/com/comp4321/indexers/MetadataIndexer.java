package com.comp4321.indexers;

import java.io.IOException;

import com.comp4321.jdbm.SafeHTree;

public class MetadataIndexer {
    private final SafeHTree<Integer, Metadata> metadataMap;
    private final int maxPages;

    public MetadataIndexer(SafeHTree<Integer, Metadata> metadataMap, int maxPages) {
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
        for (final var entry : metadataMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();
    }
}
