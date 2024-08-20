package TableAttr;
import BTree.*;


public class Page {
    private static int nMaxRows;
    private Tuple[] tuples;

    public Page() {
        tuples = new Tuple[nMaxRows];
    }
    public static void setnMaxRows(int nMaxRows) {
        Page.nMaxRows = nMaxRows;
    }
    public static int getnMaxRows() {
        return Page.nMaxRows;
    }

    public String toString() {
        String str = "";
        for (int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) continue;
            str += tuples[i].toString() + ",";
        }
        return str.substring(0, str.length() - 1);
    }

    public boolean tupleFound(String clusteringKey, Tuple tuple, ObjWrapper obj) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) {
                obj.gapRow = i;
                obj.gapPage = 1;
            }
            if(tuples[i] != null && tuples[i].compare(clusteringKey, tuple)) {
                if(obj.gapPage != -1) {
                    obj.newRow = (i == 0 ? nMaxRows - 1 : i - 1);
                    obj.samePage = i != 0;
                }else
                    obj.newRow = i;
                return true;
            }
        }
        return false;
    }
    public boolean getNextGap(ObjWrapper obj) {
        for(int i = obj.newRow; i < nMaxRows; i++) {
            if(tuples[i] == null) {
                obj.gapRow = i;
                return true;
            }
        }
        return false;
    }
    public void insertAllTuples(String colName, BTree btree) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) continue;
            //safe casting since it's already validated when inserted to table
            btree.insert((Comparable)tuples[i].getColValue(colName), tuples[i]);
        }
    }
    public Tuple getTuple(int index) {
        return tuples[index];
    }
    public void getLastGap(ObjWrapper obj) {
        for(int i = nMaxRows - 1; i >= 0; i--) {
            if(tuples[i] == null) {
                obj.gapRow = i;
                obj.gapPage = 1;
            }else {
                obj.newRow = -2;
                break;
            }
        }
    }
    public void getLastRecord(ObjWrapper obj) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] != null) {
                obj.newRow = i;
            }
        }
    }
    public boolean deleteRecord(Tuple tuple) {
        for(int i = 0; i < nMaxRows; i++) {
            if (tuples[i] != null && tuples[i].isSame(tuple)) {
                tuples[i] = null;
                return true;
            }
        }
        return false;
    }
    public boolean isEmpty() {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] != null) return false;
        }
        return true;
    }
    public void insertRecord(int index, Tuple tuple) {
        tuples[index] = tuple;
    }
}
