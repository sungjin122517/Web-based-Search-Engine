package com.comp4321.indexers;

import java.io.IOException;
import java.util.Comparator;

import com.comp4321.jdbm.SafeBTree;

import jdbm.RecordManager;

public class URLIndexer {
    public static final String URL_TO_DOCID = "urlToDocId";
    public static final String DOCID_TO_URL = "docIdToUrl";

    private final SafeBTree<String, Integer> urlToDocIdMap;
    private final SafeBTree<Integer, String> docIdToUrlMap;

    public URLIndexer(SafeBTree<String, Integer> urlToDocIdMap, SafeBTree<Integer, String> docIdToUrlMap) {
        this.urlToDocIdMap = urlToDocIdMap;
        this.docIdToUrlMap = docIdToUrlMap;
    }

    public URLIndexer(RecordManager recman) throws IOException {
        this(new SafeBTree<String, Integer>(recman, URL_TO_DOCID, Comparator.naturalOrder()),
                new SafeBTree<Integer, String>(recman, DOCID_TO_URL, Comparator.naturalOrder()));
    }

    /**
     * Retrieves the document ID associated with the given URL. If the URL is not
     * found in the index, a new document ID is created and associated with the URL.
     * We don't support removing URLs from the index, since we can't guarantee that
     * all references to the URL have been removed.
     *
     * @param url The URL for which to retrieve or create a document ID.
     * @return The document ID associated with the URL.
     * @throws IndexerException If an error occurs while retrieving or creating the
     *                          document ID.
     */
    public Integer getOrCreateDocumentId(String url) {
        try {
            final var value = urlToDocIdMap.find(url);
            if (value != null)
                return value;

            final var docId = urlToDocIdMap.size() + 1;
            urlToDocIdMap.insert(url, docId);
            docIdToUrlMap.insert(docId, url);
            return docId;
        } catch (IOException e) {
            throw new IndexerException(url, e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("URL_TO_DOCID:");
        for (final var urlTuple : urlToDocIdMap)
            System.out.println(urlTuple.getKey() + " -> " + urlTuple.getValue());
        System.out.println();

        System.out.println("DOCID_TO_URL:");
        for (final var docIdTuple : docIdToUrlMap)
            System.out.println(docIdTuple.getKey() + " -> " + docIdTuple.getValue());
        System.out.println();
    }
}
