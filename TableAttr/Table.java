package TableAttr;
import java.util.Hashtable;
import java.util.Map;
import BTree.*;


public class Table {
    private int pagesSize;
    private Page[] pages;
    private Hashtable<String, BTree> indices;

    public Table() {
        this.pagesSize = 0;
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
        //testing
        DBBTreeIterator<Integer, Integer> iterator = new DBBTreeIterator(btree);
        iterator.print();
    }

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

    private boolean validateRecord(Hashtable<String, Object> htblColNameValue, Hashtable<String,String> htblColNameType) {

        for (Map.Entry<String, Object> entry : htblColNameValue.entrySet()) {
            if(!isTypeMatching(entry.getValue(), htblColNameType.get(entry.getKey()))) {
               return false;
            }
        }
        return true;
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
    }

    public void insertRecord(String strClusteringKey, Hashtable<String,Object> htblColNameValue,
                             Hashtable<String,String> htblColNameType) {
        Hashtable<String, Object> colNameValue = new Hashtable<>(htblColNameValue);
        if(validateRecord(colNameValue, htblColNameType)) {
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
        }else {
            System.out.println("Invalid Record");
        }
    }

    private void insertInBTree(Tuple tuple) {
        if(indices == null) return;
        Hashtable<String, Object> tupleHash = tuple.getTuple();
        for(Map.Entry<String, Object> entry : tupleHash.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if(indices.contains(key))
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
            pages[i].getLastGap(obj);
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
}
