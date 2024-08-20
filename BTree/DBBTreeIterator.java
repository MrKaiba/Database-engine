package BTree;

import java.util.Iterator;
import java.util.List;

public class DBBTreeIterator<TKey extends Comparable<TKey>, TValue> implements Iterator<TValue> {
    private BTreeLeafNode<TKey, TValue> currentLeaf;
    private int currentIndex;
    private int valueIndex;

    public DBBTreeIterator(BTree<TKey, TValue> tree) {
        this.currentLeaf = tree.getSmallest();
        this.currentIndex = 0;
        this.valueIndex = 0;
    }

    @Override
    public boolean hasNext() {
        if (currentLeaf == null) {
            return false;
        }
        List<TValue> currentValueList = currentLeaf.getValue(currentIndex);
        if (valueIndex < currentValueList.size()) {
            return true;
        }
        if (currentIndex < currentLeaf.getKeyCount() - 1) {
            return true;
        }
        return currentLeaf.getNext() != null;
    }

    @Override
    public TValue next() {
        List<TValue> currentValueList = currentLeaf.getValue(currentIndex);
        TValue nextValue = currentValueList.get(valueIndex++);

        if (valueIndex >= currentValueList.size()) {
            valueIndex = 0;
            currentIndex++;
        }

        if (currentIndex >= currentLeaf.getKeyCount()) {
            currentLeaf = currentLeaf.getNext();
            currentIndex = 0;
        }

        return nextValue;
    }

    public void print() {
        while (currentLeaf != null) {
            for (int i = 0; i < currentLeaf.getKeyCount(); i++) {
                List<TValue> currentValueList = currentLeaf.getValue(i);
                for (TValue value : currentValueList) {
                    System.out.print(value + " ");
                }
            }
            System.out.println();
            currentLeaf = currentLeaf.getNext();
            currentIndex = 0;
            valueIndex = 0;
        }
    }
}
