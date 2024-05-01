package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.htmlparser.util.ParserException;
import org.mockito.Mockito;

import com.comp4321.IRUtilities.Crawler;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Example;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

public class IndexerTest {
    private RecordManager recman;
    private Indexer indexer;

    @BeforeProperty
    public void setup() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        indexer = new Indexer(recman);
    }

    @AfterProperty
    public void teardown() {
        try {
            recman.close();
            Files.deleteIfExists(Path.of("test.db"));
            Files.deleteIfExists(Path.of("test.lg"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Example
    public void addAndSearch() throws IOException, ParserException {
        final var spy = Mockito.spy(new Crawler("invalid://test.com"));

        Mockito.doReturn(List.of("hello", "world")).when(spy).extractWords();
        Mockito.doReturn(List.of("this", "is", "a", "test"), List.of("this", "is", "a", "test")).when(spy)
                .extractTitle(Mockito.anyBoolean());
        Mockito.doReturn(List.of("invalid://child.com")).when(spy).extractLinks();
        Mockito.doReturn(ZonedDateTime.now()).when(spy).getLastModified();
        Mockito.doReturn(42L).when(spy).getPageSize();

        indexer.indexDocument(spy);

        Assertions.assertThat(indexer.search(Set.of("hello"), List.of()))
                .extractingFromEntries(e -> e.getValue().url())
                .containsExactlyElementsOf(List.of("invalid://test.com"));
        Assertions.assertThat(indexer.search(Set.of("hello", "world"), List.of()))
                .extractingFromEntries(e -> e.getValue().url())
                .containsExactlyElementsOf(List.of("invalid://test.com"));
        Assertions.assertThat(indexer.search(Set.of("this", "is", "a", "test"), List.of()))
                .extractingFromEntries(e -> e.getValue().url())
                .containsExactlyElementsOf(List.of("invalid://test.com"));
        Assertions.assertThat(indexer.search(Set.of("hello"), List.of("this", "is", "a", "test")))
                .extractingFromEntries(e -> e.getValue().url())
                .containsExactlyElementsOf(List.of("invalid://test.com"));
        Assertions.assertThat(indexer.search(Set.of("hello"), List.of("world", "hello")))
                .extractingFromEntries(e -> e.getValue().url())
                .isEmpty();
    }
}
