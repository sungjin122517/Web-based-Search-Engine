package com.comp4321.indexers;

import java.io.IOException;

import jdbm.btree.BTree;
import jdbm.helper.Tuple;

public class URLIndexer {
    private final BTree urlMap;
    private final int maxPages;

    public URLIndexer(BTree urlMap, int maxPages) {
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
                urlMap.insert(url, docId, false);
            return docId;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("URL_MAP:");
        final var urlBrowser = urlMap.browse();
        Tuple urlTuple = new Tuple();
        while (urlBrowser.getNext(urlTuple))
            System.out.println(urlTuple.getKey() + " -> " + urlTuple.getValue());
        System.out.println();
    }
}
