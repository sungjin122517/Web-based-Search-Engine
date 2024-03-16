package com.comp4321.indexers;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.comp4321.jdbm.SafeHTree;

import jdbm.RecordManager;

public class LinkIndexer {
    public static final String PARENT_TO_CHILD = "parentToChild";
    public static final String CHILD_TO_PARENT = "childToParent";

    private final SafeHTree<Integer, Set<Integer>> parentToChildMap;
    private final SafeHTree<Integer, Set<Integer>> childToParentMap;

    public LinkIndexer(SafeHTree<Integer, Set<Integer>> parentToChildMap,
            SafeHTree<Integer, Set<Integer>> childToParentMap) {
        this.parentToChildMap = parentToChildMap;
        this.childToParentMap = childToParentMap;
    }

    public LinkIndexer(RecordManager recman) throws IOException {
        this(new SafeHTree<>(recman, PARENT_TO_CHILD), new SafeHTree<>(recman, CHILD_TO_PARENT));
    }

    private void addChildLink(int parent, int child) {
        if (parent == child)
            return;

        final var childKey = Integer.valueOf(child);

        try {
            var parentsValue = childToParentMap.get(childKey);
            if (parentsValue == null)
                parentsValue = new HashSet<Integer>();

            parentsValue.add(parent);
            childToParentMap.put(childKey, parentsValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void removeChildLink(int parent, int child) {
        if (parent == child)
            return;

        final var childKey = Integer.valueOf(child);

        try {
            final var parentsValue = childToParentMap.get(childKey);
            if (parentsValue == null)
                return;

            parentsValue.remove(parent);
            childToParentMap.put(childKey, parentsValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds links for a given document ID to the index.
     * If links already exist for the document, they will be removed and replaced
     * 
     * @param docId The ID of the document.
     * @param links The set of links to be added.
     */
    public void addLinks(int docId, Set<Integer> links) {
        try {
            removeLinks(docId);
            parentToChildMap.put(Integer.valueOf(docId), links);
            links.stream().forEach(child -> addChildLink(docId, child));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Removes the links associated with the given document ID.
     *
     * @param docId the ID of the document whose links are to be removed
     */
    public void removeLinks(int docId) {
        try {
            // Remove child links
            final var oldLinks = parentToChildMap.get(Integer.valueOf(docId));
            if (oldLinks != null)
                oldLinks.stream().forEach(child -> removeChildLink(docId, child));

            // Remove parent link
            parentToChildMap.remove(Integer.valueOf(docId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void printAll() throws IOException {
        System.out.println("PARENT_TO_CHILD:");
        for (final var entry : parentToChildMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();

        System.out.println("CHILD_TO_PARENT:");
        for (final var entry : childToParentMap) {
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        System.out.println();
    }
}
