package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import org.assertj.core.api.Assertions;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Example;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

public class LinkIndexerTest {
    private RecordManager recman;
    private LinkIndexer linkIndexer;

    @BeforeProperty
    public void setup() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        linkIndexer = new LinkIndexer(recman);
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
    public void testSimpleTree() throws IOException {
        // 1 -> [2, 3]
        // 2 -> [4]
        linkIndexer.addLinks(1, Set.of(2, 3));
        linkIndexer.addLinks(2, Set.of(4));

        Assertions.assertThat(linkIndexer.getChildLinks(1)).containsExactlyElementsOf(Set.of(2, 3));
        Assertions.assertThat(linkIndexer.getChildLinks(2)).containsExactlyElementsOf(Set.of(4));
        Assertions.assertThat(linkIndexer.getChildLinks(3)).isEmpty();
        Assertions.assertThat(linkIndexer.getChildLinks(4)).isEmpty();
        Assertions.assertThat(linkIndexer.getChildLinks(5)).isEmpty();

        Assertions.assertThat(linkIndexer.getParentLinks(1)).isEmpty();
        Assertions.assertThat(linkIndexer.getParentLinks(2)).containsExactlyElementsOf(Set.of(1));
        Assertions.assertThat(linkIndexer.getParentLinks(3)).containsExactlyElementsOf(Set.of(1));
        Assertions.assertThat(linkIndexer.getParentLinks(4)).containsExactlyElementsOf(Set.of(2));
        Assertions.assertThat(linkIndexer.getParentLinks(5)).isEmpty();
    }

    @Example
    public void testSimpleGraph() throws IOException {
        // 1 -> [2, 3]
        // 2 -> [3, 4]
        // 3 -> [1, 4]
        // 4 -> [2, 3]
        linkIndexer.addLinks(1, Set.of(2, 3));
        linkIndexer.addLinks(2, Set.of(3, 4));
        linkIndexer.addLinks(3, Set.of(1, 4));
        linkIndexer.addLinks(4, Set.of(2, 3));

        Assertions.assertThat(linkIndexer.getChildLinks(1)).containsExactlyInAnyOrderElementsOf(Set.of(2, 3));
        Assertions.assertThat(linkIndexer.getChildLinks(2)).containsExactlyInAnyOrderElementsOf(Set.of(3, 4));
        Assertions.assertThat(linkIndexer.getChildLinks(3)).containsExactlyInAnyOrderElementsOf(Set.of(1, 4));
        Assertions.assertThat(linkIndexer.getChildLinks(4)).containsExactlyInAnyOrderElementsOf(Set.of(2, 3));
        Assertions.assertThat(linkIndexer.getChildLinks(5)).isEmpty();

        Assertions.assertThat(linkIndexer.getParentLinks(1)).containsExactlyInAnyOrderElementsOf(Set.of(3));
        Assertions.assertThat(linkIndexer.getParentLinks(2)).containsExactlyInAnyOrderElementsOf(Set.of(1, 4));
        Assertions.assertThat(linkIndexer.getParentLinks(3)).containsExactlyInAnyOrderElementsOf(Set.of(1, 2, 4));
        Assertions.assertThat(linkIndexer.getParentLinks(4)).containsExactlyInAnyOrderElementsOf(Set.of(2, 3));
        Assertions.assertThat(linkIndexer.getParentLinks(5)).isEmpty();
    }

    @Example
    public void testSelfLoop() throws IOException {
        // 1 -> [1]
        linkIndexer.addLinks(1, Set.of(1));

        Assertions.assertThat(linkIndexer.getChildLinks(1)).containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(linkIndexer.getParentLinks(1)).containsExactlyInAnyOrderElementsOf(Set.of(1));
    }

    @Example
    public void testOverwrite() throws IOException {
        // 1 -> [2, 3]
        // 2 -> [4]
        // 2 -> [3]
        linkIndexer.addLinks(1, Set.of(2, 3));
        linkIndexer.addLinks(2, Set.of(4));
        linkIndexer.addLinks(2, Set.of(3));

        Assertions.assertThat(linkIndexer.getChildLinks(1)).containsExactlyInAnyOrderElementsOf(Set.of(2, 3));
        Assertions.assertThat(linkIndexer.getChildLinks(2)).containsExactlyInAnyOrderElementsOf(Set.of(3));
        Assertions.assertThat(linkIndexer.getChildLinks(3)).isEmpty();
        Assertions.assertThat(linkIndexer.getChildLinks(4)).isEmpty();
        Assertions.assertThat(linkIndexer.getChildLinks(5)).isEmpty();

        Assertions.assertThat(linkIndexer.getParentLinks(1)).isEmpty();
        Assertions.assertThat(linkIndexer.getParentLinks(2)).containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(linkIndexer.getParentLinks(3)).containsExactlyInAnyOrderElementsOf(Set.of(1, 2));
        Assertions.assertThat(linkIndexer.getParentLinks(4)).isEmpty();
    }
}
