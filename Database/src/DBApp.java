import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DBApp {
	
	public void init() {
		
	}
	
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)throws DBAppException, IOException{
		Table newTable = new Table(strTableName);
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream("./data/tables.class",true);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(newTable);
	         out.close();
	         fileOut.close();
	         System.out.printf("success");
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
		FileWriter csvWriter = new FileWriter("./data/metadata.csv",true);
		Set<String> keys = htblColNameType.keySet();
		String attribute;
		String dataType;
		for(String key : keys) {
			attribute = key;
			dataType = htblColNameType.get(key);
			csvWriter.append(strTableName);
			csvWriter.append(",");
			csvWriter.append(attribute);
			csvWriter.append(",");
			csvWriter.append(dataType);
			csvWriter.append(",");
			System.out.println("!");
			if(attribute.equals(strClusteringKeyColumn))
				csvWriter.append("True");
			else
				csvWriter.append("False");
			csvWriter.append(",");
			csvWriter.append("False"); //Until indexing in milestone 2
			csvWriter.append("\n");
			
		}
		csvWriter.flush();
		csvWriter.close();
		
		
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
	public static void main(String[] args) throws DBAppException, IOException {
		String strTableName = "Student";
		DBApp dbApp = new DBApp( );
		Hashtable htblColNameType = new Hashtable( );
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.double");
		dbApp.createTable( strTableName, "id", htblColNameType );
	}
	
	
}
