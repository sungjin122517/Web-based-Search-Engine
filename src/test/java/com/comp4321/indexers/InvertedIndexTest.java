package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.assertj.core.api.Assertions;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Example;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

public class InvertedIndexTest {
    private RecordManager recman;
    private InvertedIndex invertedIndex;

    @BeforeProperty
    public void setup() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        invertedIndex = new InvertedIndex(recman);
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
    public void checkScores() throws IOException {
        invertedIndex.addDocument(TestDocument.titleOnlyDocument.docId(), TestDocument.titleOnlyDocument.titleIds(),
                TestDocument.titleOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.bodyOnlyDocument.docId(), TestDocument.bodyOnlyDocument.titleIds(),
                TestDocument.bodyOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.mixedDocument.docId(), TestDocument.mixedDocument.titleIds(),
                TestDocument.mixedDocument.bodyIds());

        // score = (0.9 * title_tf + 0.1 * body_tf) * log(N / df) / tf_max / ||doc||
        // doc1:
        // (1.8 log 3) / (2 sqrt((log 3)^2 + 2 (0.5 log 3)^2)) =
        // 0.7348469228349534294591852224117674175897842441970010385298
        // doc2:
        // (0.2 log 3) / (2 sqrt((log 3)^2 + 2 (0.5 log 3)^2)) =
        // 0.0816496580927726032732428024901963797321982493552223376144
        // doc3:
        // (log 3) / (sqrt(6 (log 3)^2) =
        // 0.4082482904638630163662140124509818986609912467761116880721

        // Check scores with precision of 1e-10
        final var scores = invertedIndex.getScores(Set.of(1, 4, 7, 10));
        Assertions.assertThat(scores.get(1)).isCloseTo(0.7348469228349534294591852224117674175897842441970010385298,
                Assertions.within(1e-10));
        Assertions.assertThat(scores.get(2)).isCloseTo(0.0816496580927726032732428024901963797321982493552223376144,
                Assertions.within(1e-10));
        Assertions.assertThat(scores.get(3)).isCloseTo(0.4082482904638630163662140124509818986609912467761116880721,
                Assertions.within(1e-10));
    }

    @Example
    public void checkFrequency() throws IOException {
        invertedIndex.addDocument(TestDocument.titleOnlyDocument.docId(), TestDocument.titleOnlyDocument.titleIds(),
                TestDocument.titleOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.bodyOnlyDocument.docId(), TestDocument.bodyOnlyDocument.titleIds(),
                TestDocument.bodyOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.mixedDocument.docId(), TestDocument.mixedDocument.titleIds(),
                TestDocument.mixedDocument.bodyIds());

        // Check frequency
        Assertions.assertThat(invertedIndex.getKeywordsWithFrequency(1))
                .containsExactlyInAnyOrderEntriesOf(Map.of(1, 2, 2, 1, 3, 1));
        Assertions.assertThat(invertedIndex.getKeywordsWithFrequency(2))
                .containsExactlyInAnyOrderEntriesOf(Map.of(4, 2, 5, 1, 6, 1));
        Assertions.assertThat(invertedIndex.getKeywordsWithFrequency(3))
                .containsExactlyInAnyOrderEntriesOf(Map.of(7, 1, 8, 1, 9, 1, 10, 1, 11, 1, 12, 1));
        Assertions.assertThat(invertedIndex.getKeywordsWithFrequency(4)).isEmpty();
    }

    @Example
    public void checkPhrase() throws IOException {
        invertedIndex.addDocument(TestDocument.titleOnlyDocument.docId(), TestDocument.titleOnlyDocument.titleIds(),
                TestDocument.titleOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.bodyOnlyDocument.docId(), TestDocument.bodyOnlyDocument.titleIds(),
                TestDocument.bodyOnlyDocument.bodyIds());
        invertedIndex.addDocument(TestDocument.mixedDocument.docId(), TestDocument.mixedDocument.titleIds(),
                TestDocument.mixedDocument.bodyIds());

        // Check phrase
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1, 1)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1, 1, 2)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1, 1, 2, 3)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1, 2)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(1, 2, 3)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(2)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(2, 3)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
        Assertions.assertThat(invertedIndex.getDocumentsWithPhrase(List.of(3)))
                .containsExactlyInAnyOrderElementsOf(Set.of(1));
    }
}
