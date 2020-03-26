import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class DBApp {
	
	public void init() {
		
	}
	
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)throws DBAppException{
		
	}
	
	public void insertIntoTable(String strTableName,
			 Hashtable<String,Object> htblColNameValue)
			 throws DBAppException{
		
	}
	public void updateTable(String strTableName,
			 String strClusteringKey,
			Hashtable<String,Object> htblColNameValue )
			throws DBAppException {
		
	}
	public void deleteFromTable(String strTableName,
			 Hashtable<String,Object> htblColNameValue)
			 throws DBAppException {
		
	}
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
		String[] strarrOperators)
		throws DBAppException {
			Iterator i = null;
			return i;
	}

	
	
}
