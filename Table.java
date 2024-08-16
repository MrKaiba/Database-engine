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
    private void shiftUp(String clusteringKey, Tuple tuple, ObjWrapper obj) {
        int nMax = Page.getnMaxRows();
        int stRow = (obj.gapRow == nMax - 1 ? 0 : obj.gapRow + 1);
        int stPage = (obj.gapRow == nMax - 1 ? obj.gapPage + 1 : obj.gapPage);
        for(int i = stPage; i <= obj.newPage; i++) {
            for(int j = (i == stPage ? stRow : 0); j <= (i == obj.newPage ? obj.newRow : pagesSize); j++) {
                int prevPage = (j == 0 ? i - 1 : i);
                int prevRow = (j == 0 ? nMax - 1 : j - 1);
                pages[prevPage].insertRecord(clusteringKey, prevRow, tuple);
            }
        }
    }
    private void shiftDown(String clusteringKey, Tuple tuple, ObjWrapper obj) {

    }
    public void insertRecord(String strClusteringKey, Hashtable<String,Object> htblColNameValue,
                             Hashtable<String,String> htblColNameType) {
        if(validateRecord(htblColNameValue, htblColNameType)) {
            Tuple tuple = new Tuple(htblColNameValue);
            ObjWrapper obj = new ObjWrapper();
            for(int i = 0; i < pagesSize; i++) {
                if(pages[i].tupleFound(strClusteringKey, tuple, obj)) {
                    obj.newRow = i;
                    break;
                }
            }
            if(!obj.samePage)
                obj.newPage--;
            //if gapPage != -1
            if(~obj.gapPage > 0) {
                shiftUp(strClusteringKey, tuple, obj);
            }else {
                shiftDown(strClusteringKey, tuple, obj);
            }
        }else {
            System.out.println("Invalid Type");
        }
    }
}
