package com.comp4321.indexers;

import org.assertj.core.api.Assertions;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.state.Action;
import net.jqwik.api.state.ActionChain;
import net.jqwik.api.state.Transformer;

public class PostingTest {
    @Provide
    public Arbitrary<ActionChain<Posting>> actions() {
        return ActionChain.startWith(() -> new Posting(0))
                .withAction(new AddTitleLocationAction())
                .withAction(new AddBodyLocationAction());
    }

    @Property
    public void checkIndex(@ForAll("actions") ActionChain<Posting> actions) {
        actions.run();
    }
}

class AddTitleLocationAction implements Action.Independent<Posting> {
    @Override
    public Arbitrary<Transformer<Posting>> transformer() {
        final var titleLocation = Arbitraries.integers().greaterOrEqual(0);
        return titleLocation.map(loc -> Transformer.transform(
                String.format("Add title location %d", loc),
                posting -> {
                    final var beforeSize = posting.titleLocations().size();

                    final var newPosting = posting.addTitleLocation(loc);
                    Assertions.assertThat(newPosting.titleLocations())
                            .describedAs("Title locations should contain %d", loc)
                            .contains(loc);
                    Assertions.assertThat(newPosting.titleLocations())
                            .describedAs("Title locations should contain all previous locations")
                            .containsAll(posting.titleLocations());
                    Assertions.assertThat(posting.titleLocations())
                            .describedAs("Original title locations should not be modified")
                            .hasSize(beforeSize);

                    return newPosting;
                }));
    }
}

class AddBodyLocationAction implements Action.Independent<Posting> {
    @Override
    public Arbitrary<Transformer<Posting>> transformer() {
        final var bodyLocation = Arbitraries.integers().greaterOrEqual(0);
        return bodyLocation.map(loc -> Transformer.transform(
                String.format("Add body location %d", loc),
                posting -> {
                    final var beforeSize = posting.bodyLocations().size();

                    final var newPosting = posting.addBodyLocation(loc);
                    Assertions.assertThat(newPosting.bodyLocations())
                            .describedAs("Body locations should contain %d", loc)
                            .contains(loc);
                    Assertions.assertThat(newPosting.bodyLocations())
                            .describedAs("Body locations should contain all previous locations")
                            .containsAll(posting.bodyLocations());
                    Assertions.assertThat(posting.bodyLocations())
                            .describedAs("Original body locations should not be modified")
                            .hasSize(beforeSize);

                    return newPosting;
                }));
    }
}
