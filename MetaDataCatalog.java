import java.util.Hashtable;

class MetaData {
    Hashtable<String,String> htblColNameType;
    String strClusteringKeyColumn;
    String strReferencedTable;
    String strReferencedColumn;
    String strReferencingColumn;
}
public class MetaDataCatalog {
    private final Hashtable<String, MetaData> tablesMetaData;

    public MetaDataCatalog() {
        tablesMetaData = new Hashtable<>();
    }

    public void addTableMetaData(String tableName, Hashtable<String,String> htblColNameType,
                                 String strClusteringKeyColumn, String strReferencedTable, String strReferencedColumn,
                                 String strReferencingColumn) {
        MetaData metaData = new MetaData();
        metaData.htblColNameType = new Hashtable<>(htblColNameType);
        metaData.strClusteringKeyColumn = strClusteringKeyColumn;
        metaData.strReferencedTable = strReferencedTable;
        metaData.strReferencedColumn = strReferencedColumn;
        metaData.strReferencingColumn = strReferencingColumn;
        tablesMetaData.put(tableName, metaData);
    }
    public String getClusteringKeyColumn(String tableName) {
        return tablesMetaData.get(tableName).strClusteringKeyColumn;
    }
    public Hashtable<String,String> gethtblColNameType(String tableName) {
        return tablesMetaData.get(tableName).htblColNameType;
    }
    public String getReferencedTable(String tableName) {
        return tablesMetaData.get(tableName).strReferencedTable;
    }
    public String getReferencedColumn(String tableName) {
        return tablesMetaData.get(tableName).strReferencedColumn;
    }
    public String getReferencingColumn(String tableName) {
        return tablesMetaData.get(tableName).strReferencingColumn;
    }
}
