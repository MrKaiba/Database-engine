
/** * @author Wael Abouelsaadat */ 

import java.util.Iterator;
import java.util.Hashtable;


public class DBApp {



	public DBApp( ){
		
	}

	// init does whatever initialization you would like. It takes as input 
	// the number of rows/tuples per page. 
	public void init( int nMaximumRowsinPage ){
		
		
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
	// and will be passed in the Hashtable htblColNameType..
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
									
	
	
	}



	// following method creates a B+ tree index on specifid 
	// column in specified table.
	public void createIndex(String strTableName,
							String strColName,
							String strIndexName) throws DBAppException {
								
	
	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	// Referential integrity constraints pertaining any foreign/primary 
	// relation must be respected; else an exception is thrown.
	public void insertIntoTable(String strTableName,
								Hashtable<String,Object> htblColNameValue)
								throws DBAppException{
	
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
	
	}

	// following method is used to join any number of tables. Created B+ 
	// trees must be used if there is an opportunity for them to be used.
	// Iterator is java.util.Iterator It is an interface that enables 
	// client code to iterate over the results row by row. Whatever object 
	// you return holding the result set, it should implement the Iterator 
	// interface.
	public Iterator join( String[] strarrTableNames )	throws DBAppException{
		return null;
	}

	// following method is used to dump a whole table, i.e. all the rows 
	// are printed to the screen. 
	public void dumpTable( String strTable ) throws DBAppException{
		
	}

	// following method is used to dump a specific page in a specific 
	// table table. What is passed is the page index in the array.
	public void dumpPage( String strTable, int nPageNumber ) throws DBAppException {
		
	}

	public static void main( String[] args ){
	
	try{
			DBApp	dbApp = new DBApp( );
			
			String strTableName = "";
			Hashtable htblColNameType= null;
			Hashtable htblColNameValue = null;
				
			strTableName = "Major";
			htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("major", "java.lang.String");
			dbApp.createTable( strTableName, htblColNameType, "id", null, null, null );
			dbApp.createIndex( strTableName, "id", "major_id_Index" );
						
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 1 ));
			htblColNameValue.put("major", new String( "CSEN" ) );			
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 2 ));
			htblColNameValue.put("major", new String( "DMET" ) );			
			dbApp.insertIntoTable( strTableName , htblColNameValue );
			
			htblColNameValue = new Hashtable( );
			htblColNameValue.put("id", Integer.valueOf( 3 ));
			htblColNameValue.put("major", new String( "BI" ) );			
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
			
			strTableName = "Student";
			htblColNameType = new Hashtable( );
			htblColNameType.put("id", "java.lang.Integer");
			htblColNameType.put("name", "java.lang.String");
			htblColNameType.put("gpa", "java.lang.double");
			htblColNameType.put("majorID", "java.lang.Integer");
			dbApp.createTable( strTableName, htblColNameType, "id", "Major","id", "majorID" );
			dbApp.createIndex( strTableName, "id", "student_id_Index" );
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
			strTables = new String[2];
			strTables[0]  = "Major";
			strTables[1]  = "Student";
			Iterator it = dbApp.join( strTables );
			
		}
		catch(Exception exp){
			exp.printStackTrace( );
		}
	}

}