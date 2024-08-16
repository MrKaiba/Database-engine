import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map;

public class Table {
    private int pagesSize;
    private Page[] pages;
    public Table() {
        this.pagesSize = 0;
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
        for(int i = 0; i < pagesSize; i++) {
            if(pages[i] != null) {
                if(pages[i].deleteRecord(tuple)) {
                    if (pages[i].isEmpty()) pages[i] = null;
                    return;
                }
            }
        }
    }

    public void insertRecord(String strClusteringKey, Hashtable<String,Object> htblColNameValue,
                             Hashtable<String,String> htblColNameType) {
        if(validateRecord(htblColNameValue, htblColNameType)) {
            Tuple tuple = new Tuple(htblColNameValue);
            ObjWrapper obj = new ObjWrapper();
            int prevGap = -1;
            for(int i = 0; i < pagesSize; i++) {
                if(obj.gapPage != prevGap) {
                    prevGap = i;
                    obj.gapPage = i;
                }
                if(pages[i].tupleFound(strClusteringKey, tuple, obj)) {
                    if(obj.gapPage != prevGap) {
                        obj.gapPage = i;
                    }
                    obj.newPage = i;
                    break;
                }
            }
            insert(obj, tuple);
        }else {
            System.out.println("Invalid Type");
        }
    }

    private boolean getLastGap(ObjWrapper obj) {
        int savedGapPage = obj.gapPage;
        int savedGapRow = obj.gapRow;
        obj.gapPage = obj.gapRow = obj.newRow = -1;
        int prevGap = -1;
        for(int i = pagesSize - 1; i >= 0; i--) {
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
                obj.newPage--;
            int stRow = (obj.gapRow == nMax - 1 ? 0 : obj.gapRow + 1);
            int stPage = (obj.gapRow == nMax - 1 ? obj.gapPage + 1 : obj.gapPage);
            shiftUp(obj, stRow, stPage);
        }else if(obj.newRow != -1) {
            getNextGap(obj);
            if(obj.gapPage == -1) {
                insertInNewPage();
                obj.gapRow = 0;
                obj.gapPage = pagesSize - 1;
            }
            int stRow = (obj.gapRow == 0 ? nMax - 1 : obj.gapRow - 1);
            int stPage = (obj.gapRow == 0 ? obj.gapPage - 1 : obj.gapPage);
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

    private void getNextGap(ObjWrapper obj) {
        for(int i = obj.newPage; i < pagesSize; i++) {
            if(pages[i].getNextGap(obj)) {
                obj.gapPage = i;
                break;
            }
        }
    }

    private void insertLast(ObjWrapper obj) {
        int stRow = (obj.gapRow == Page.getnMaxRows() - 1 ? 0 : obj.gapRow + 1);
        int stPage = (obj.gapRow == Page.getnMaxRows() - 1 ? obj.gapPage + 1 : obj.gapPage);
        for(int i = Page.getnMaxRows() - 1; i >= 0; i--) {
            if(!pages[i].isEmpty()) {
                obj.newPage = i;
                break;
            }
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
            for(int j = (i == stPage ? stRow : 0); j <= (i == obj.newPage ? obj.newRow : pagesSize - 1); j++) {
                int prevPage = (j == 0 ? i - 1 : i);
                int prevRow = (j == 0 ? nMax - 1 : j - 1);
                pages[prevPage].insertRecord(prevRow, pages[i].getTuple(j));
            }
        }
    }

    private void shiftDown(ObjWrapper obj, int stRow, int stPage) {
        if(obj.gapRow == -1)
            insertInNewPage();
        int nMax = Page.getnMaxRows();
        for(int i = stPage; i >= obj.newPage; i--) {
            for(int j = (i == stPage ? stRow : nMax - 1); j >= (i == obj.newPage ? obj.newRow : 0); j--) {
                int nextPage = (j == nMax - 1 ? i + 1 : i);
                int nextRow = (j == nMax - 1 ? 0 : j + 1);
                pages[nextPage].insertRecord(nextRow, pages[i].getTuple(j));
            }
        }
    }
}
