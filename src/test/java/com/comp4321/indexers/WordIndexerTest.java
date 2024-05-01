package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.assertj.core.api.Assertions;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.NotEmpty;
import net.jqwik.api.constraints.UniqueElements;

public class WordIndexerTest implements AutoCloseable {
    private final RecordManager recman;
    private final WordIndexer wordIndexer;

    public WordIndexerTest() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        wordIndexer = new WordIndexer(recman);
    }

    @Property
    public void getNonExistentWordReturnsNull(@ForAll Integer wordId) throws IOException {
        Assertions.assertThat(wordIndexer.getWord(wordId)).isEmpty();
    }

    @Property
    public void createAndGetWord(@ForAll @NotEmpty String word) throws IOException {
        final var wordid = wordIndexer.getOrCreateId(word);
        final var retrievedWord = wordIndexer.getWord(wordid).get();

        Assertions.assertThat(retrievedWord).isEqualTo(word);
    }

    @Property
    public void sameWordsHaveSameId(@ForAll @NotEmpty String word) throws IOException {
        final var wordid1 = wordIndexer.getOrCreateId(word);
        final var wordid2 = wordIndexer.getOrCreateId(word);

        Assertions.assertThat(wordid1).isEqualTo(wordid2);
    }

    @Property
    public void allWordIdsAreUnique(@ForAll @UniqueElements List<@NotEmpty String> words) throws IOException {
        Assertions.assertThat(words.stream().map(word -> {
            try {
                return wordIndexer.getOrCreateId(word);
            } catch (IOException e) {
                return Assertions.fail("Failed to get or create word id", e);
            }
        }).distinct().count()).isEqualTo(words.size());
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
