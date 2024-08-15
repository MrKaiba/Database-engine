import java.util.Hashtable;

class MetaData {
    Hashtable<String,String> htblColNameType;
    String strClusteringKeyColumn;
    String strReferencedTable;
    String strReferencedColumn;
    String strReferencingColumn;
}
public class MetaDataCatalog {
    Hashtable<String, MetaData> tablesMetaData;

    public MetaDataCatalog() {
        tablesMetaData = new Hashtable<>();
    }

    public void addTableMetaData(String tableName, Hashtable<String,String> htblColNameType,
                                 String strClusteringKeyColumn, String strReferencedTable, String strReferencedColumn,
                                 String strReferencingColumn) {
        MetaData metaData = new MetaData();
        metaData.htblColNameType = htblColNameType;
        metaData.strClusteringKeyColumn = strClusteringKeyColumn;
        metaData.strReferencedTable = strReferencedTable;
        metaData.strReferencedColumn = strReferencedColumn;
        metaData.strReferencingColumn = strReferencingColumn;
        tablesMetaData.put(tableName, metaData);
    }
}
