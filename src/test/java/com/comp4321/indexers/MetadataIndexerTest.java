package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.assertj.core.api.Assertions;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.time.api.DateTimes;

public class MetadataIndexerTest implements AutoCloseable {
    private final RecordManager recman;
    private final MetadataIndexer metadataIndexer;

    public MetadataIndexerTest() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        metadataIndexer = new MetadataIndexer(recman);
    }

    @Provide
    public Arbitrary<Metadata> metadata() {
        final var title = Arbitraries.strings().alpha().numeric().ofMinLength(1);
        final var lastModified = DateTimes.zonedDateTimes();
        final var pageSize = Arbitraries.longs().greaterOrEqual(1);

        return Combinators.combine(title, lastModified, pageSize).as(Metadata::new);
    }

    @Property
    public void addAndGetMetadata(@ForAll int docId, @ForAll("metadata") Metadata metadata) throws IOException {
        metadataIndexer.addMetadata(docId, metadata);
        final var retrievedMetadata = metadataIndexer.getMetadata(docId).get();
        Assertions.assertThat(retrievedMetadata.title()).isEqualTo(metadata.title());
        Assertions.assertThat(retrievedMetadata.lastModified()).isEqualTo(metadata.lastModified());
        Assertions.assertThat(retrievedMetadata.pageSize()).isEqualTo(metadata.pageSize());
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
