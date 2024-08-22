package TableAttr;
import BTree.*;

import java.util.ArrayList;
import java.util.List;


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
    public boolean containsPrimary(String clusteringKey, Object tupleClusteringKey) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) continue;
            if(tuples[i].getColValue(clusteringKey).equals(tupleClusteringKey)) {
                return true;
            }
        }
        return false;
    }
    public Tuple foundTuple(String colName, Object value) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) continue;
            Object obj = tuples[i].getColValue(colName);
            if(obj != null && obj.equals(value)) {
                return tuples[i];
            }
        }
        return null;
    }
    public void join(Table otherTable, ArrayList<Tuple> tuplesList, BTree otherBTree, String referencedCol, String referencingCol) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) continue;
            Object value = tuples[i].getColValue(referencingCol);
            if(otherBTree == null) {
                Tuple otherTuple = otherTable.findTuple(referencedCol, value);
                if(otherTuple == null) continue;
                tuplesList.add(tuples[i].joinTuples(otherTuple, referencedCol));
                continue;
            }
            List<Tuple> otherTuple = otherBTree.search((Comparable)value);
            if(otherTuple == null) continue;
            tuplesList.add(tuples[i].joinTuples(otherTuple.getFirst(), referencedCol));
        }
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