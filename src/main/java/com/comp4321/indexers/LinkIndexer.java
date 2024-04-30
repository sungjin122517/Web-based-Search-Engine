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

    private void addChildLink(int parent, int child) throws IOException {
        var parentsValue = childToParentMap.get(child);
        if (parentsValue == null)
            parentsValue = new HashSet<>();

        parentsValue.add(parent);
        childToParentMap.put(child, parentsValue);
    }

    private void removeChildLink(int parent, int child) throws IOException {
        final var parentsValue = childToParentMap.get(child);
        if (parentsValue == null)
            return;

        parentsValue.remove(parent);

        // Update the child's parent links (if it's not empty)
        if (parentsValue.isEmpty())
            childToParentMap.remove(child);
        else
            childToParentMap.put(child, parentsValue);
    }

    /**
     * Adds links for a given document ID to the index.
     * If links already exist for the document, they will be removed and replaced
     * 
     * @param docId The ID of the document.
     * @param links The set of links to be added.
     */
    public void addLinks(int docId, Set<Integer> links) throws IOException {
        removeLinks(docId);
        parentToChildMap.put(docId, links);
        links.stream().forEach(child -> {
            try {
                addChildLink(docId, child);
            } catch (IOException e) {
                throw new IndexerException(String.format("DocId: %d", docId), e);
            }
        });
    }

    /**
     * Removes the links associated with the given document ID.
     *
     * @param docId the ID of the document whose links are to be removed
     */
    public void removeLinks(int docId) throws IOException {
        final var oldLinks = parentToChildMap.get(docId);
        if (oldLinks == null)
            return;

        // Remove child links
        oldLinks.stream().forEach(child -> {
            try {
                removeChildLink(docId, child);
            } catch (IOException e) {
                throw new IndexerException(String.format("DocId: %d", docId), e);
            }
        });

        // Remove parent link
        parentToChildMap.remove(docId);
    }

    /**
     * Retrieves the parent links for a given document ID.
     *
     * @param docId the document ID for which to retrieve the parent links
     * @return a set of integers representing the parent links
     * @throws IOException if an I/O error occurs while retrieving the parent links
     */
    public Set<Integer> getParentLinks(Integer docId) throws IOException {
        final var parentLinks = childToParentMap.get(docId);
        if (parentLinks == null)
            return Set.of();

        return parentLinks;
    }

    /**
     * Retrieves the child links associated with the given document ID.
     *
     * @param docId the document ID for which to retrieve the child links
     * @return a set of integers representing the child links
     * @throws IOException if an I/O error occurs while retrieving the child links
     */
    public Set<Integer> getChildLinks(Integer docId) throws IOException {
        final var childLinks = parentToChildMap.get(docId);
        if (childLinks == null)
            return Set.of();

        return childLinks;
    }

    public void printAll() {
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
