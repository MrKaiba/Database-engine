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
    public boolean tupleFound(String clusteringKey, Tuple tuple, ObjWrapper obj) {
        for(int i = 0; i < nMaxRows; i++) {
            if(tuples[i] == null) {
                obj.gapRow = i;
            }
            if(tuples[i] != null && tuples[i].compare(clusteringKey, tuple)) {
                obj.newRow = (i == 0 ? nMaxRows - 1 : i - 1);
                obj.samePage = i != 0;
                return true;
            }
        }
        return false;
    }
    public boolean getNextGap(ObjWrapper obj) {
        return true;
    }
    public void insertRecord(String strClusteringKey, int index, Tuple tuple) {
        tuples[index] = tuple;
    }
}
