package TableAttr;
import java.util.*;

import BTree.*;


public class Table {
    private int pagesSize;
    private Page[] pages;
    private Hashtable<String, BTree> indices;

    public Table() {
        this.pagesSize = 0;
    }

    public static void setnMaxRows(int nMaxRows) {
        Page.setnMaxRows(nMaxRows);
    }

    public void createIndex(String colName) {
        if (indices == null) {
            indices = new Hashtable<>();
        }
        BTree btree = new BTree();
        indices.put(colName, btree);
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            pages[i].insertAllTuples(colName, btree);
        }
    }
    public boolean checkIndex(String colName) {
         return (indices != null && indices.containsKey(colName));
    }
    @Override
    public String toString() {
        String str = "";
        for(int i = 0; i < pagesSize; i++) {
            str += pages[i].toString() + "\n";
        }
        return str;
    }
    public String dumpPage(int index) {
        String str = "Index out of bounds";
        if(index >= 0 && index < pagesSize) {
            str = "Null page";
            if(pages[index] != null) {
                str = pages[index].toString();
            }
        }
        return str;
    }

    public boolean validateRecord(Hashtable<String, Object> htblColNameValue, String strClusteringKey, Hashtable<String,String> htblColNameType) {
        if(htblColNameValue == null)
            return false;
        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!isTypeMatching(entry.getValue(), htblColNameType.get(entry.getKey())))
                return false;
        }
        if(htblColNameValue.get(strClusteringKey) == null)
            return false;
        if(tuplePrimaryExists(strClusteringKey, htblColNameValue.get(strClusteringKey)))
            return false;

        return true;
    }

    private boolean tuplePrimaryExists(String clusteringKey, Object tupleClusteringKey) {
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            if(pages[i].containsPrimary(clusteringKey, tupleClusteringKey)) {
                return true;
            }
        }
        return false;
    }

    private boolean isTypeMatching(Object value, String expectedType) {
        try {
            Class<?> expectedClass = Class.forName(expectedType);
            return expectedClass.isInstance(value);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void deleteRecord(Hashtable<String,Object> htblColNameValue) {
        Tuple tuple = new Tuple(htblColNameValue);
        deleteFromBTree(tuple);
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            if(pages[i].deleteRecord(tuple)) {
                if (pages[i].isEmpty()) pages[i] = null;
                return;
            }
        }
        System.out.println("Element not found so nothing is deleted!");
    }
    public void insertRecord(String strClusteringKey, Hashtable<String,Object> htblColNameValue) {
        Hashtable<String, Object> colNameValue = new Hashtable<>(htblColNameValue);
        Tuple tuple = new Tuple(colNameValue);
        ObjWrapper obj = new ObjWrapper();
        int prevGap = -1;
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            if(obj.gapPage != prevGap) {
                int tempPrevGap = getPrevPage(i);
                prevGap = tempPrevGap;
                obj.gapPage = tempPrevGap;
            }
            if(pages[i].tupleFound(strClusteringKey, tuple, obj)) {
                if(obj.gapPage != prevGap) {
                    obj.gapPage = i;
                }
                obj.newPage = i;
                break;
            }
        }
        insertInBTree(tuple);
        insert(obj, tuple);
    }

    private void insertInBTree(Tuple tuple) {
        if(indices == null) return;
        Hashtable<String, Object> tupleHash = tuple.getTuple();
        for(Map.Entry<String, Object> entry : tupleHash.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(indices.containsKey(key))
                indices.get(key).insert((Comparable)value, tuple);
        }
    }

    private void deleteFromBTree(Tuple tuple) {
        if(indices == null) return;
        Hashtable<String, Object> tupleHash = tuple.getTuple();
        for(Map.Entry<String, Object> entry : tupleHash.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(indices.contains(key))
                indices.get(key).delete((Comparable)value, tuple);
        }
    }

    private boolean getLastGap(ObjWrapper obj) {
        int savedGapPage = obj.gapPage;
        int savedGapRow = obj.gapRow;
        obj.gapPage = obj.gapRow = obj.newRow = -1;
        int prevGap = -1;
        for(int i = pagesSize - 1; i >= 0; i--) {
            if(pages[i] == null) continue;
            pages[i].getFirstGap(obj);
            if(obj.gapPage != prevGap) {
                prevGap = obj.gapPage = i;
            }
            if(obj.newRow == -2) break;
        }
        if(obj.gapRow != -1) {
            obj.newRow = obj.gapRow;
            obj.newPage = obj.gapPage;
            return true;
        }
        obj.gapPage = savedGapPage;
        obj.gapRow = savedGapRow;
        return false;
    }
    /* handling insertion cases as follows
        1-found tuple greater than or equal to the passed tuple and found a gap before it(Shift Up).
        2-Same as 1st case but did not find a gap before it which is separated into two cases:
            2.1-found no gaps in the whole table(Create new page).
            2.2-found a gap after the tuple.(Shift Down).
        3-found a gap but the inserted tuple has the largest value, and it's separated into two cases:
            3.1-found a gap after the last present element.(Normal insertion in that gap).
            3.2-found a gap between the present elements(Shift Up).
        4-No gaps found in the whole table(Create new page).
     */
    private void insert(ObjWrapper obj, Tuple tuple) {
        int nMax = Page.getnMaxRows();
        if(obj.gapRow != -1 && obj.newRow != -1) {
            if(!obj.samePage)
                obj.newPage = getPrevPage(obj.newPage);
            int stRow = (obj.gapRow == nMax - 1 ? 0 : obj.gapRow + 1);
            int stPage = (obj.gapRow == nMax - 1 ? getNextPage(obj.gapPage) : obj.gapPage);
            shiftUp(obj, stRow, stPage);
        }else if(obj.newRow != -1) {
            getNextGap(obj);
            if(obj.gapPage == -1) {
                insertInNewPage();
                obj.gapRow = 0;
                obj.gapPage = pagesSize - 1;
            }
            int stRow = (obj.gapRow == 0 ? nMax - 1 : obj.gapRow - 1);
            int stPage = (obj.gapRow == 0 ? getPrevPage(obj.gapPage) : obj.gapPage);
            shiftDown(obj, stRow, stPage);
        }else if(obj.gapRow != -1) {
            boolean lastExists = getLastGap(obj);
            if(!lastExists) {
                insertLast(obj);
            }
        }else {
            insertInNewPage();
            obj.newPage = pagesSize - 1;
            obj.newRow = 0;
        }
        pages[obj.newPage].insertRecord(obj.newRow, tuple);
    }

    private int getPrevPage(int curPage) {
        int newPage = -1;
        for(int i = curPage - 1; i >= 0; i--) {
            if(pages[i] != null) {
                newPage = i;
                break;
            }
        }
        return newPage;
    }
    private int getNextPage(int curPage) {
        int newPage = -1;
        for(int i = curPage + 1; i < pagesSize; i++) {
            if(pages[i] != null) {
                newPage = i;
                break;
            }
        }
        return newPage;
    }
    private void getNextGap(ObjWrapper obj) {
        for(int i = obj.newPage; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            if(pages[i].getNextGap(obj)) {
                obj.gapPage = i;
                break;
            }
        }
    }

    private void insertLast(ObjWrapper obj) {
        int stRow = (obj.gapRow == Page.getnMaxRows() - 1 ? 0 : obj.gapRow + 1);
        int stPage = (obj.gapRow == Page.getnMaxRows() - 1 ? getNextPage(obj.gapPage) : obj.gapPage);
        for(int i = pagesSize - 1; i >= 0; i--) {
            if(pages[i] == null) continue;
            obj.newPage = i;
            break;
        }
        pages[obj.newPage].getLastRecord(obj);
        shiftUp(obj, stRow, stPage);
    }

    private void insertInNewPage() {
        Page[] newPages = new Page[pagesSize + 1];
        newPages[pagesSize] = new Page();
        for(int i = 0; i < pagesSize; i++) {
            newPages[i] = pages[i];
            pages[i] = null;
        }
        pages = newPages;
        pagesSize++;
    }

    private void shiftUp(ObjWrapper obj, int stRow, int stPage) {
        int nMax = Page.getnMaxRows();
        for(int i = stPage; i <= obj.newPage; i++) {
            if(pages[i] == null) continue;
            for(int j = (i == stPage ? stRow : 0); j <= (i == obj.newPage ? obj.newRow : nMax - 1); j++) {
                int prevPage = (j == 0 ? getPrevPage(i) : i);
                int prevRow = (j == 0 ? nMax - 1 : j - 1);
                pages[prevPage].insertRecord(prevRow, pages[i].getTuple(j));
            }
        }
    }

    private void shiftDown(ObjWrapper obj, int stRow, int stPage) {
        int nMax = Page.getnMaxRows();
        for(int i = stPage; i >= obj.newPage; i--) {
            if(pages[i] == null) continue;
            for(int j = (i == stPage ? stRow : nMax - 1); j >= (i == obj.newPage ? obj.newRow : 0); j--) {
                int nextPage = (j == nMax - 1 ? getNextPage(i) : i);
                int nextRow = (j == nMax - 1 ? 0 : j + 1);
                pages[nextPage].insertRecord(nextRow, pages[i].getTuple(j));
            }
        }
    }
    public void join(Table otherTable, ArrayList<Tuple> tuples, String referencedCol, String referencingCol) {
        if(tuples.isEmpty())
            joinFirstTwo(otherTable, tuples, referencedCol, referencingCol);
        else
            BTreeJoin(otherTable, tuples, referencedCol, referencingCol, tuples.iterator());
    }

    private void joinFirstTwo(Table otherTable, ArrayList<Tuple> tuples, String referencedCol, String referencingCol) {
        if (indices == null)
            linearJoin(otherTable, tuples, referencedCol, referencingCol);
        else if (indices.containsKey(referencingCol)) {
            BTree btree = indices.get(referencingCol);
            DBBTreeIterator iterator = new DBBTreeIterator(btree);
            BTreeJoin(otherTable, tuples, referencedCol, referencingCol, iterator);
        } else {
            BTree btree = indices.entrySet().iterator().next().getValue();
            DBBTreeIterator iterator = new DBBTreeIterator(btree);
            BTreeJoin(otherTable, tuples, referencedCol, referencingCol, iterator);
        }
    }

    private void BTreeJoin(Table otherTable, ArrayList<Tuple> tuples, String referencedCol, String referencingCol, Iterator iterator) {
        BTree otherBTree = otherTable.getBTree(referencedCol);
        ArrayList<Tuple> addedTuples = new ArrayList<>();

        while(iterator.hasNext()) {
            Tuple tuple = (Tuple)iterator.next();
            Object value = tuple.getColValue(referencingCol);
            if(otherBTree == null) {
                Tuple otherTuple = otherTable.findTuple(referencedCol, value);
                if(otherTuple == null) continue;
                addedTuples.add(tuple.joinTuples(otherTuple, referencedCol));
                continue;
            }
            List<Tuple> otherTuple = otherBTree.search((Comparable)value);
            if(otherTuple == null) continue;
            addedTuples.add(tuple.joinTuples(otherTuple.getFirst(), referencedCol));
        }
        tuples.clear();
        tuples.addAll(addedTuples);
    }

    private void linearJoin(Table otherTable, ArrayList<Tuple> tuples, String referencedCol, String referencingCol) {
        BTree otherBTree = otherTable.getBTree(referencedCol);
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            pages[i].join(otherTable, tuples, otherBTree, referencedCol, referencingCol);
        }
    }

    public Tuple findTuple(String colName, Object value) {
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            Tuple foundTuple = pages[i].foundTuple(colName, value);
            if(foundTuple != null) return foundTuple;
        }
        return null;
    }
    private BTree getBTree(String colName) {
        if(indices == null || !indices.containsKey(colName)) return null;
        return indices.get(colName);
    }
    public boolean containsVal(Object value, String colName) {
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] == null) continue;
            Tuple foundTuple = pages[i].foundTuple(colName, value);
            if(foundTuple != null) return true;
        }
        return false;
    }
}