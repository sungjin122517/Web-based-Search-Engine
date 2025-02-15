package com.comp4321.indexers;

import java.io.IOException;
import java.util.Optional;

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class MetadataIndexer {
    public static final String DOCID_TO_METADATA = "docIdToMetadata";

    private final SafeHTree<Integer, Metadata> metadataMap;

    public MetadataIndexer(SafeHTree<Integer, Metadata> metadataMap) {
        this.metadataMap = metadataMap;
    }

    public MetadataIndexer(RecordManager recman) throws IOException {
        this(new SafeHTree<>(recman, DOCID_TO_METADATA));
    }

    /**
     * Get the metadata for a document with the specified document ID.
     * 
     * @param docId The document ID.
     * @return The metadata for the document, or null if no metadata exists.
     * @throws IOException if an error occurs while getting the metadata.
     */
    public Optional<Metadata> getMetadata(int docId) throws IOException {
        return Optional.ofNullable(metadataMap.get(docId));
    }

    /**
     * Adds metadata for a document with the specified document ID.
     * If metadata already exists for the document, it will be removed and replaced
     * with the new metadata.
     *
     * @param docId    The document ID.
     * @param metadata The metadata to be added.
     * @throws IOException if an error occurs while adding the metadata.
     */
    public void addMetadata(int docId, Metadata metadata) throws IOException {
        metadataMap.put(docId, metadata);
    }

    public void printAll() {
        System.out.println("DOCID_TO_METADATA:");
        for (final var entry : metadataMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();
    }
}
