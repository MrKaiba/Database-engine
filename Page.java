public class Page {
    private static int nMaxRows;
    private int size;
    private Tuple[] tuples;

    public Page() {
        tuples = new Tuple[nMaxRows];
        size = 0;
    }
    public static void setnMaxRows(int nMaxRows) {
        Page.nMaxRows = nMaxRows;
    }
}
