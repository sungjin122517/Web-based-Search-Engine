package com.comp4321.indexers;

import java.io.IOException;

import com.comp4321.jdbm.SafeBTree;

public class URLIndexer {
    private final SafeBTree<String, Integer> urlMap;
    private final int maxPages;

    public URLIndexer(SafeBTree<String, Integer> urlMap, int maxPages) {
        this.urlMap = urlMap;
        this.maxPages = maxPages;
    }

    public int getOrCreateDocumentId(String url) {
        try {
            final var value = urlMap.find(url);
            if (value != null)
                return (int) value;

            final var docId = urlMap.size() + 1;
            if (docId <= maxPages)
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
