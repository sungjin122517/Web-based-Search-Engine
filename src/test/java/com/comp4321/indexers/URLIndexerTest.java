package com.comp4321.indexers;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.assertj.core.api.Assertions;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

public class URLIndexerTest implements AutoCloseable {
    private final RecordManager recman;
    private final URLIndexer urlIndexer;

    public URLIndexerTest() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        urlIndexer = new URLIndexer(recman);
    }

    @Provide
    public Arbitrary<URL> urls() {
        return Arbitraries.strings().alpha().numeric().ofMinLength(1)
                .<URL>map(s -> {
                    try {
                        return URI.create("http://example.com/" + s).toURL();
                    } catch (MalformedURLException e) {
                        return Assertions.fail("Failed to create URL", e);
                    }
                });
    }

    @Provide
    public Arbitrary<List<URL>> uniqueUrls() {
        return urls().list().uniqueElements();
    }

    @Property
    public void getNonExistentURLThrowsException(@ForAll Integer docId) throws IOException {
        Assertions.assertThat(urlIndexer.getURL(docId)).isEmpty();
    }

    @Property
    public void createAndGetURL(@ForAll("urls") URL url) throws IOException {
        final var docId = urlIndexer.getOrCreateDocumentId(url.toString());
        final var retrievedUrl = urlIndexer.getURL(docId).get();

        Assertions.assertThat(retrievedUrl).isEqualTo(url.toString());
    }

    @Property
    public void sameURLsHaveSameId(@ForAll("urls") URL url) throws IOException {
        final var docId1 = urlIndexer.getOrCreateDocumentId(url.toString());
        final var docId2 = urlIndexer.getOrCreateDocumentId(url.toString());

        Assertions.assertThat(docId1).isEqualTo(docId2);
    }

    @Property
    public void allDocumentIdsAreUnique(@ForAll("uniqueUrls") List<URL> urls) throws IOException {
        Assertions.assertThat(urls.stream().map(url -> {
            try {
                return urlIndexer.getOrCreateDocumentId(url.toString());
            } catch (IOException e) {
                return Assertions.fail("Failed to get or create document id", e);
            }
        }).distinct().count()).isEqualTo(urls.size());
    }

    @Override
    public void close() {
        try {
            recman.close();
            Files.deleteIfExists(Path.of("test.db"));
            Files.deleteIfExists(Path.of("test.lg"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
