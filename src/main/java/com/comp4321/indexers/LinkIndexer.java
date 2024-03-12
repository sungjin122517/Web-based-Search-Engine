package com.comp4321.indexers;

import java.io.IOException;
import java.util.ArrayList;

import jdbm.htree.HTree;

public class LinkIndexer {
    private final HTree parentToChild;
    private final HTree childToParent;

    public LinkIndexer(HTree parentToChild, HTree childToParent) {
        this.parentToChild = parentToChild;
        this.childToParent = childToParent;
    }

    public void addLink(int parent, int child) {
        final var parentKey = Integer.valueOf(parent);
        final var childKey = Integer.valueOf(child);

        try {
            @SuppressWarnings("unchecked")
            var childValue = (ArrayList<Integer>) parentToChild.get(parentKey);
            if (childValue == null)
                childValue = new ArrayList<Integer>();

            childValue.add(child);
            parentToChild.put(parentKey, childValue);

            @SuppressWarnings("unchecked")
            var parentValue = (ArrayList<Integer>) childToParent.get(childKey);
            if (parentValue == null)
                parentValue = new ArrayList<Integer>();

            parentValue.add(parent);
            childToParent.put(childKey, parentValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("PARENT_TO_CHILD:");
        final var parentKeys = parentToChild.keys();
        var parentKey = (Integer) parentKeys.next();
        while (parentKey != null) {
            @SuppressWarnings("unchecked")
            final var childEntry = (ArrayList<Integer>) parentToChild.get(parentKey);
            System.out.println(parentKey + " -> " + childEntry.toString());

            parentKey = (Integer) parentKeys.next();
        }
        System.out.println();

        System.out.println("CHILD_TO_PARENT:");
        final var childKeys = childToParent.keys();
        var childKey = (Integer) childKeys.next();
        while (childKey != null) {
            @SuppressWarnings("unchecked")
            final var parentEntry = (ArrayList<Integer>) childToParent.get(childKey);
            System.out.println(childKey + " -> " + parentEntry.toString());

            childKey = (Integer) childKeys.next();
        }
        System.out.println();
    }
}
