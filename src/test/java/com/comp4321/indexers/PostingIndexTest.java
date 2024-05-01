package com.comp4321.indexers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;
import jdbm.RecordManagerFactory;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.state.Action;
import net.jqwik.api.state.ActionChain;
import net.jqwik.api.state.Transformer;

public class PostingIndexTest implements AutoCloseable {
    private RecordManager recman;
    private PostingIndex index;

    public PostingIndexTest() throws IOException {
        recman = RecordManagerFactory.createRecordManager("test");
        index = new PostingIndex("test", new SafeHTree<>(recman, "forwardIndex"),
                new SafeHTree<>(recman, "invertedIndex"));
    }

    public PostingIndex index() {
        return index;
    }

    @Provide
    public Arbitrary<ActionChain<PostingIndex>> actions() {
        return ActionChain.startWith(this::index).withAction(new AddDocumentAction());
    }

    @Property
    public void checkIndex(@ForAll("actions") ActionChain<PostingIndex> actions) {
        actions.run();
    }

    @Override
    public void close() throws Exception {
        try {
            recman.close();
            recman = null;
            Files.deleteIfExists(Path.of("test.db"));
            Files.deleteIfExists(Path.of("test.lg"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

class AddDocumentAction implements Action.Dependent<PostingIndex> {
    @Override
    public Arbitrary<Transformer<PostingIndex>> transformer(PostingIndex index) {
        final var docId = Arbitraries.integers().greaterOrEqual(1);
        final var titleIds = Arbitraries.integers()
                .greaterOrEqual(1)
                .list()
                .ofMinSize(1)
                .ofMaxSize(10)
                .uniqueElements();
        final var bodyIds = Arbitraries.integers()
                .greaterOrEqual(1)
                .list()
                .ofMinSize(1)
                .ofMaxSize(10)
                .uniqueElements();

        final var documents = Combinators.combine(docId, titleIds, bodyIds).as(TestDocument::new);

        return documents.map(document -> Transformer.mutate(
                String.format("Add document %d, Body Ids: %s, Title Ids: %s",
                        document.docId(),
                        document.bodyIds().toString(),
                        document.titleIds().toString()),
                curIndex -> {
                    // Remove the document if it already exists
                    try {
                        curIndex.removeDocument(document.docId());
                    } catch (IOException e) {
                        Assertions.fail("Failed to remove document %d", document.docId());
                    }

                    final var prevTitleDF = document.titleIds().stream()
                            .collect(Collectors.toMap(id -> id, id -> {
                                try {
                                    final var postings = curIndex.getPostings(id);
                                    return postings == null ? 0 : postings.size();
                                } catch (IOException e) {
                                    return Assertions.fail("Failed to get DF for title word %d", id);
                                }
                            }));
                    final var prevBodyDF = document.bodyIds().stream()
                            .collect(Collectors.toMap(id -> id, id -> {
                                try {
                                    final var postings = curIndex.getPostings(id);
                                    return postings == null ? 0 : postings.size();
                                } catch (IOException e) {
                                    return Assertions.fail("Failed to get DF for body word %d", id);
                                }
                            }));

                    try {
                        curIndex.addDocument(document.docId(), document.titleIds(), document.bodyIds());

                        final var totalWords = index.getForwardWords(document.docId());
                        final var addedWords = new HashSet<>(document.titleIds());
                        addedWords.addAll(document.bodyIds());
                        Assertions.assertThat(totalWords)
                                .describedAs("Total words for document %d", document.docId())
                                .containsExactlyInAnyOrderElementsOf(addedWords);
                    } catch (IOException e) {
                        Assertions.fail("Failed to add document %d", document.docId());
                    }

                    prevTitleDF.forEach((id, df) -> {
                        try {
                            Assertions.assertThat(curIndex.getDF(id))
                                    .describedAs("DF for title word %d", id)
                                    .isEqualTo(df + 1);
                        } catch (IOException e) {
                            Assertions.fail("Failed to get DF for title word %d", id);
                        }
                    });
                    prevBodyDF.forEach((id, df) -> {
                        try {
                            Assertions.assertThat(curIndex.getDF(id))
                                    .describedAs("DF for body word %d", id)
                                    .isEqualTo(df + 1);
                        } catch (IOException e) {
                            Assertions.fail("Failed to get DF for body word %d", id);
                        }
                    });

                }));
    }
}