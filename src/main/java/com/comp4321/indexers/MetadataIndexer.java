package com.comp4321.indexers;

import java.io.IOException;

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class MetadataIndexer {
    public static final String METADATA_MAP = "metadataMap";

    private final SafeHTree<Integer, Metadata> metadataMap;

    public MetadataIndexer(SafeHTree<Integer, Metadata> metadataMap) {
        this.metadataMap = metadataMap;
    }

    public MetadataIndexer(RecordManager recman) throws IOException {
        this(new SafeHTree<>(recman, METADATA_MAP));
    }

    public Metadata getMetadata(int docId) {
        try {
            return (Metadata) metadataMap.get(docId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addMetadata(int docId, Metadata metadata) {
        try {
            removeMetadata(docId);
            metadataMap.put(docId, metadata);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeMetadata(int docId) {
        try {
            metadataMap.remove(docId);
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
