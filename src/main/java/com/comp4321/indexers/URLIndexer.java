package com.comp4321.indexers;

import java.io.IOException;
import java.util.Comparator;

import com.comp4321.jdbm.SafeBTree;

import jdbm.RecordManager;

public class URLIndexer {
    public static final String URL_MAP = "urlMap";
    private final SafeBTree<String, Integer> urlMap;

    public URLIndexer(SafeBTree<String, Integer> urlMap) {
        this.urlMap = urlMap;
    }

    public URLIndexer(RecordManager recman) throws IOException {
        this(new SafeBTree<String, Integer>(recman, URL_MAP, Comparator.naturalOrder()));
    }

    /**
     * Retrieves the document ID associated with the given URL. If the URL is not
     * found in the index, a new document ID is created and associated with the URL.
     * We don't support removing URLs from the index, since we can't guarantee that
     * all references to the URL have been removed.
     *
     * @param url The URL for which to retrieve or create a document ID.
     * @return The document ID associated with the URL.
     * @throws RuntimeException If an error occurs while retrieving or creating the
     *                          document ID.
     */
    public int getOrCreateDocumentId(String url) {
        try {
            final var value = urlMap.find(url);
            if (value != null)
                return (int) value;

            final var docId = urlMap.size() + 1;
            urlMap.insert(url, docId);
            return docId;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("URL_MAP:");
        for (final var urlTuple : urlMap)
            System.out.println(urlTuple.getKey() + " -> " + urlTuple.getValue());
        System.out.println();
    }
}
