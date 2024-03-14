package com.comp4321.jdbm;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;

import jdbm.RecordManager;
import jdbm.helper.FastIterator;
import jdbm.htree.HTree;

public class SafeHTree<K, V> implements Iterable<Entry<K, V>> {
    private final HTree htree;

    public SafeHTree(RecordManager recman, String name) throws IOException {
        long recid = recman.getNamedObject(name);
        if (recid != 0) {
            htree = HTree.load(recman, recid);
        } else {
            htree = HTree.createInstance(recman);
            recman.setNamedObject(name, htree.getRecid());
        }
    }

    public V get(K key) throws IOException {
        @SuppressWarnings("unchecked")
        final var value = (V) htree.get(key);
        return value;
    }

    public void put(K key, V value) throws IOException {
        htree.put(key, value);
    }

    public void remove(K key) throws IOException {
        htree.remove(key);
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        try {
            return new Iterator<Entry<K, V>>() {
                private final FastIterator keyIt = htree.keys();
                private final Queue<K> keys = new ArrayDeque<K>();

                @Override
                public boolean hasNext() {
                    if (!keys.isEmpty())
                        return true;

                    @SuppressWarnings("unchecked")
                    final var curKey = (K) keyIt.next();
                    if (curKey == null)
                        return false;

                    keys.add(curKey);
                    return true;
                }

                @Override
                public Entry<K, V> next() {
                    try {
                        if (!hasNext())
                            throw new NoSuchElementException();

                        return new Entry<K, V>() {
                            private final K key = keys.remove();
                            private final V value = get(key);

                            @Override
                            public K getKey() {
                                return key;
                            }

                            @Override
                            public V getValue() {
                                return value;
                            }

                            @Override
                            public V setValue(V value) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public boolean equals(Object obj) {
                                if (obj == null || !(obj instanceof Entry))
                                    return false;

                                return getKey().equals(((Entry<?, ?>) obj).getKey())
                                        && getValue().equals(((Entry<?, ?>) obj).getValue());
                            }

                            @Override
                            public int hashCode() {
                                return Objects.hash(getKey(), getValue());
                            }
                        };

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
