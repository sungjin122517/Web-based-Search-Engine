package com.comp4321.indexers;

import java.time.ZonedDateTime;

import org.assertj.core.api.Assertions;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.NotEmpty;
import net.jqwik.time.api.DateTimes;

public class MetadataTest {
    @Provide
    public Arbitrary<Metadata> metadata() {
        final var title = Arbitraries.strings().alpha().numeric().ofMinLength(1);
        final var lastModified = DateTimes.zonedDateTimes();
        final var pageSize = Arbitraries.longs().greaterOrEqual(1);

        return Combinators.combine(title, lastModified, pageSize).as(Metadata::new);
    }

    @Property
    public void createAndGetMetadata(@ForAll @NotEmpty String title, @ForAll ZonedDateTime lastModified,
            @ForAll long pageSize) {
        Assertions.assertThat(new Metadata(title, lastModified, pageSize))
                .extracting("title", "lastModified", "pageSize")
                .containsExactly(title, lastModified, pageSize);
    }
}
