/** * @author Wael Abouelsaadat */

import TableAttr.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Hashtable;
import java.util.Map;


public class DBApp {

	Hashtable<String, Table> tables;
	MetaDataCatalog metaDataCatalog;

	public DBApp( ){
		init(3);
	}

	// init does whatever initialization you would like. It takes as input
	// the number of rows/tuples per page.
	public void init( int nMaximumRowsinPage ) {
		Table.setnMaxRows(nMaximumRowsinPage);
		tables = new Hashtable<>();
		metaDataCatalog = new MetaDataCatalog();
	}

	// htblColNameValue will have the column name as key and the data
	// type as value
	// strClusteringKeyColumn is the name of the column that will be
	// the primary key and the clustering column as well. The data type
	// of that column will be passed in htblColNameType
	// strReferencedTable and strReferencedColumn are the names of
	// another Table and column, respectively, that are being referenced
	// by strReferencingColumn.
	// strReferencingColumn is one of the columns belonging to this table
	// and will be passed in the Hashtable htblColNameType.
	// This method will throw an Exception if specified strReferencedTable
	// and or strReferencedColumn do not exist, or different data type
	// than strReferencingColumn.
	// If no reference to another table exists, last three parameters are
	// passed null.

	public void createTable(String strTableName,
							Hashtable<String,String> htblColNameType,
							String strClusteringKeyColumn,
							String strReferencedTable,
							String strReferencedColumn,
							String strReferencingColumn )
			throws DBAppException{
		if(tables.containsKey(strTableName)) {
			System.out.println("Table " + strTableName + " already exists");
			return;
		}
		validateTable(htblColNameType, strReferencedTable, strReferencedColumn, strReferencingColumn);

		metaDataCatalog.addTableMetaData(strTableName, htblColNameType, strClusteringKeyColumn,
				strReferencedTable, strReferencedColumn, strReferencingColumn);

		Table table = new Table();
		tables.put(strTableName, table);
	}

	private void validateTable(  Hashtable<String,String> htblColNameType,
								 String strReferencedTable,
								 String strReferencedColumn,
								 String strReferencingColumn ) throws DBAppException {
		if(strReferencedColumn != null && strReferencedTable != null && strReferencingColumn != null) {

			if (!tables.containsKey(strReferencedTable)) {
				throw new DBAppException("Table " + strReferencedTable + " does not exist");
			}
			Hashtable<String, String> refHashTable = metaDataCatalog.gethtblColNameType(strReferencedTable);
			if (!refHashTable.containsKey(strReferencedColumn)) {
				throw new DBAppException("Column " + strReferencedColumn + " does not exist");
			}
			String curDataType = htblColNameType.get(strReferencingColumn);
			String otherDataType = refHashTable.get(strReferencedColumn);
			if (!curDataType.equals(otherDataType)) {
				throw new DBAppException("Column " + strReferencingColumn + " does not match");
			}
		}else if(!(strReferencedColumn == null && strReferencedTable == null && strReferencingColumn == null)){
			throw new DBAppException("Parameters don't match");
		}
	}


	// following method creates a B+ tree index on specified
	// column in specified table.
	public void createIndex(String strTableName,
							String strColName,
							String strIndexName) throws DBAppException {
		if(!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " does not exist");
		Table table = tables.get(strTableName);
		if(!metaDataCatalog.gethtblColNameType(strTableName).containsKey(strColName))
			throw new DBAppException("Column " + strColName + " does not exist");
		if(table.checkIndex(strColName))
			throw new DBAppException("Index already exists");
		table.createIndex(strColName);
	}


	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	// Referential integrity constraints pertaining any foreign/primary
	// relation must be respected; else an exception is thrown.
	public void insertIntoTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
			throws DBAppException{
		if(!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " does not exist");
		Table table = tables.get(strTableName);
		String clusteringKey = metaDataCatalog.getClusteringKeyColumn(strTableName);
		Hashtable<String, String>htblColNameType = metaDataCatalog.gethtblColNameType(strTableName);
		if(!table.validateRecord(htblColNameValue, clusteringKey, htblColNameType))
			throw new DBAppException("Insertion failed due to invalid record");
		table.insertRecord(clusteringKey, htblColNameValue);
	}


	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in
	// search to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	// Referential integrity constraints pertaining any foreign/primary
	// relation must be respected; else an exception is thrown.
	// This is not a cascaded delete.
	public void deleteFromTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
			throws DBAppException{
		if(!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " does not exist");
		Table table = tables.get(strTableName);
		for(Map.Entry<String, Table> entry : tables.entrySet()) {
			String key = entry.getKey();
			if(key.equals(strTableName) || metaDataCatalog.getReferencedTable(key) == null) continue;
			Table otherTable = entry.getValue();
			if(metaDataCatalog.getReferencedTable(key).equals(strTableName)) {
				if(otherTable.containsVal(htblColNameValue.get(metaDataCatalog.getReferencedColumn(key)), metaDataCatalog.getReferencingColumn(key))) {
					throw new DBAppException("Can't delete record due to referential integrity constraints");
				}
			}

		}
		table.deleteRecord(htblColNameValue);
	}

	// following method is used to join any number of tables. Created B+
	// trees must be used if there is an opportunity for them to be used.
	// Iterator is java.util.Iterator It is an interface that enables
	// client code to iterate over the results row by row. Whatever object
	// you return holding the result set, it should implement the Iterator
	// interface.
	public Iterator join( String[] strarrTableNames )	throws DBAppException{
		for(int i = 0; i < strarrTableNames.length; i++){
			if(!tables.containsKey(strarrTableNames[i]))
				throw new DBAppException("Table " + strarrTableNames[i] + " does not exist");
		}
		String[] sortedTables = validateAndSort(strarrTableNames);
		if(sortedTables == null) return null;
		Table[] joinTables = new Table[sortedTables.length];
		for(int i = 0; i < sortedTables.length; i++) {
			joinTables[i] = tables.get(sortedTables[i]);
		}
		ArrayList<Tuple> it = new ArrayList<>();
		for(int i = 0; i < joinTables.length - 1; i++) {
			String referencedCol = metaDataCatalog.getReferencedColumn(sortedTables[i]);
			String referencingCol = metaDataCatalog.getReferencingColumn(sortedTables[i]);
			joinTables[i].join(joinTables[i + 1], it, referencedCol, referencingCol);
		}
		return it.iterator();
	}
	public String[] validateAndSort(String[] strarrTableNames) {
		Pair[] modTableNames = new Pair[strarrTableNames.length];
		for(int i = 0; i < strarrTableNames.length; i++) {
			String referencedTable = metaDataCatalog.getReferencedTable(strarrTableNames[i]);
			modTableNames[i] = new Pair(strarrTableNames[i], referencedTable);
		}
		Pair noReferencePair = new Pair();
		int noReferenceTablesCount = countNoReference(noReferencePair, modTableNames);
		if(noReferenceTablesCount >= 2) return null;
		return sortTopologically(noReferencePair, modTableNames);
	}
	private int countNoReference(Pair noReferencePair, Pair[] modTableNames) {
		int ret = 0;
		for(int i = 0; i < modTableNames.length; i++) {
			boolean foundReference = false;
			for(int j = 0; j < modTableNames.length; j++) if(!foundReference) {
				if(i == j) continue;
				foundReference = modTableNames[j].first.equals(modTableNames[i].second);
			}
			if(!foundReference) {
				noReferencePair.first = modTableNames[i].first;
				noReferencePair.second = modTableNames[i].second;
				ret++;
			}
		}
		return ret;
	}
	private String[] sortTopologically(Pair noReferencePair, Pair[] modTableNames) {
		String[] sortedTables = new String[modTableNames.length];
		int index = sortedTables.length - 1;
		String search = noReferencePair.first;
		sortedTables[index] = search;
		index--;
		while(index >= 0) {
			for (int i = 0; i < modTableNames.length; i++) {
				if (modTableNames[i].second != null && modTableNames[i].second.equals(search)) {
					sortedTables[index] = modTableNames[i].first;
					search = sortedTables[index];
					index--;
				}
			}
		}
		return sortedTables;
	}

	// following method is used to dump a whole table, i.e. all the rows
	// are printed to the screen.
	public void dumpTable( String strTableName ) throws DBAppException{
		if(!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " does not exist");
		System.out.println(tables.get(strTableName).toString());
	}

	// following method is used to dump a specific page in a specific
	// table. What is passed is the page index in the array.
	public void dumpPage( String strTableName, int nPageNumber ) throws DBAppException {
		if(!tables.containsKey(strTableName))
			throw new DBAppException("Table " + strTableName + " does not exist");
		System.out.println(tables.get(strTableName).dumpPage(nPageNumber));
	}

	public static void main( String[] args ){

		try{
			DBApp	dbApp = new DBApp( );

			String strTableName = "";
			Hashtable htblColNameType= null;
			Hashtable htblColNameValue = null;

			strTableName = "Course";
			htblColNameType = new Hashtable<>();
			htblColNameType.put("courseID", "java.lang.Integer");
			htblColNameType.put("courseName", "java.lang.String");
			dbApp.createTable(strTableName, htblColNameType, "courseID", null, null, null);

			dbApp.createIndex( strTableName, "courseID", "course_id_index" );

			// Inserting data into Course table
			htblColNameValue = new Hashtable<>();
			htblColNameValue.put("courseID", 101);
			htblColNameValue.put("courseName", "Data Structures");
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("courseID", 102);
			htblColNameValue.put("courseName", "Digital Design");
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("courseID", 201);
			htblColNameValue.put("courseName", "Biochemistry");
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			dbApp.dumpTable(strTableName);

			/*strTableName = "Department";
			htblColNameType = new Hashtable<>();
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("departmentName", "java.lang.String");
			htblColNameType.put("cID", "java.lang.Integer");
			dbApp.createTable(strTableName, htblColNameType, "id", "Course", "courseID", "cID");

			// Inserting data into Department table
			htblColNameValue = new Hashtable<>();
			htblColNameValue.put("id", 1);
			htblColNameValue.put("departmentName", "Engineering");
			htblColNameValue.put("cID", 101);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", 2);
			htblColNameValue.put("departmentName", "Medical Sciences");
			htblColNameValue.put("cID", 102);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("id", 3);
			htblColNameValue.put("departmentName", "Business Administration");
			htblColNameValue.put("cID", 201);
			dbApp.insertIntoTable(strTableName, htblColNameValue);

			strTableName = "Major";
			htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("major", "java.lang.String");
			htblColNameType.put("depID", "java.lang.Integer");
			dbApp.createTable( strTableName, htblColNameType, "id", "Department", "id", "depID" );
			dbApp.createIndex( strTableName, "id", "major_id_Index" );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 1 ));
			htblColNameValue.put("major", new String( "CSEN" ) );
			htblColNameValue.put("depID", Integer.valueOf( 2 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 2 ));
			htblColNameValue.put("major", new String( "DMET" ) );
			htblColNameValue.put("depID", Integer.valueOf( 2 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 3 ));
			htblColNameValue.put("major", new String( "BI" ) );
			htblColNameValue.put("depID", Integer.valueOf( 3 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 4 ));
			htblColNameValue.put("major", new String( "IET" ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 5 ));
			htblColNameValue.put("major", new String( "MECHA" ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 6 ));
			htblColNameValue.put("major", new String( "Pharma" ) );
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			//dbApp.dumpTable(strTableName);

			//delete a record.
			strTableName = "Department";
			htblColNameValue = new Hashtable<>();
			htblColNameValue.put("id", 1);
			htblColNameValue.put("departmentName", "Engineering");
			htblColNameValue.put("cID", 101);
			dbApp.deleteFromTable( strTableName , htblColNameValue );

			//dbApp.dumpPage(strTableName, 1);

			strTableName = "Student";
			htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.Double");
			htblColNameType.put("majorID", "java.lang.Integer");
			dbApp.createTable( strTableName, htblColNameType, "id", "Major","id", "majorID" );
			dbApp.createIndex( strTableName, "name", "student_id_Index" );
			dbApp.createIndex( strTableName, "gpa", "student_gpa_Index" );

			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 1 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
			htblColNameValue.put("majorID", Integer.valueOf( 1 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", Integer.valueOf( 2 ));
			htblColNameValue.put("name", new String("Ahmed Noor" ) );
			htblColNameValue.put("gpa", Double.valueOf( 0.95 ) );
			htblColNameValue.put("majorID", Integer.valueOf( 2 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", Integer.valueOf( 3 ));
			htblColNameValue.put("name", new String("Dalia Noor" ) );
			htblColNameValue.put("gpa", Double.valueOf( 1.25 ) );
			htblColNameValue.put("majorID", Integer.valueOf( 3 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", Integer.valueOf( 4 ));
			htblColNameValue.put("name", new String("John Noor" ) );
			htblColNameValue.put("gpa", Double.valueOf( 1.5 ) );
			htblColNameValue.put("majorID", Integer.valueOf( 1 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			htblColNameValue.clear( );
			htblColNameValue.put("id", Integer.valueOf( 5 ));
			htblColNameValue.put("name", new String("Zaky Noor" ) );
			htblColNameValue.put("gpa", Double.valueOf( 0.88 ) );
			htblColNameValue.put("majorID", Integer.valueOf( 2 ));
			dbApp.insertIntoTable( strTableName , htblColNameValue );

			// Note: any number of tables could be joined together.
			String[] strTables;
			strTables = new String[4];
			strTables[0]  = "Major";
			strTables[1]  = "Student";
			strTables[2] = "Department";
			strTables[3] = "Course";
			Iterator it = dbApp.join( strTables );
			while(it.hasNext( )) {
				System.out.println(it.next());
			}*/

		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}
}