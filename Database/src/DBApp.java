import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//@SuppressWarnings({ "unchecked", "unchecked" })
public class DBApp {
	
	private Vector<Table> tableVector =  new Vector<>(); //contains all table objects
	public static int currentpages = 0;
	
	//At the start of the program run this method to read all the tables from the disk and assign the vector to table vector
	public void init() {
		
	      try {
	         FileInputStream fileIn = new FileInputStream("./data/tables.class");
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         tableVector = (Vector<Table>) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	         return;
	      } catch (ClassNotFoundException c) {
	         System.out.println("Vector class not found");
	         c.printStackTrace();
	         return;
	      }
	     
	      
		
	}
	
	
	
	//when creating a table add it to the vector and rewrite the vector on the disk
	//then write the table details on the metadata
	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType)throws DBAppException, IOException{
		Table newTable = new Table(strTableName);
		tableVector.add(newTable);
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream("./data/tables.class");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(tableVector);
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
			//System.out.println("!");
			if(attribute.equals(strClusteringKeyColumn))
				csvWriter.append("True");
			else
				csvWriter.append("False");
			csvWriter.append(",");
			csvWriter.append("False"); //Until indexing in milestone 2
			csvWriter.append("\n");
			
		}
		csvWriter.append(strTableName);
		csvWriter.append(",");
		csvWriter.append("TouchDate");
		csvWriter.append(",");
		csvWriter.append("java.util.Date");
		csvWriter.append("\n");
		csvWriter.flush();
		csvWriter.close();
		
		
	}
	
	
	
	//uncompleted
	public void insertIntoTable(String strTableName,
			 Hashtable<String,Object> htblColNameValue)
			 throws DBAppException, IOException{
		Table tempTable = null;
		for(Table table : tableVector)
		{
			if(table.name.equals(strTableName))
				tempTable=table;
		}
		if(tempTable==null)
			System.out.println("Table not found");
		else {
			if(tempTable.pages.size()==0)
			{
				//first insertion in table
				Vector<Tuple> page = new Vector<>();
				Set<String> keys = htblColNameValue.keySet();
				Tuple t = null;
				String data;
				Object value;
				for(String key:keys) {
					data = key;
					String type ="class " + checkDataType(strTableName, key);
					value=htblColNameValue.get(key);
					String valueType = value.getClass()+"";
					if(type.equals(valueType))
					{
						//Data type in metafile = datatype inserted by user
						tempTable.pages.add(page);
						t = new Tuple(data, value);
						currentpages++;
						page.add(t);
						
					}
					
					
					
				}
				
				
			}
		}
		
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
	//@SuppressWarnings({ "rawtypes", "rawtypes", "rawtypes" })
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
		String[] strarrOperators)
		throws DBAppException {
			Iterator i = null;
			return i;
	}
	
	
	
	//returns the data type of an attribute in a table
	public static String checkDataType(String tableName, String attribute) throws IOException
	{
		BufferedReader br = new BufferedReader(new FileReader("./data/metadata.csv"));
		String line = br.readLine();
		while(line!=null)
		{
			String [] data = line.split(",");
			String name= data[0];
			String attr = data[1];
			String type = data[2];
			if(name.equals(tableName))
			{
				if(attr.equals(attribute));
					return type;
			}
			
			
			
			line=br.readLine();
		}
		br.close();
		return "#"; //attribute not found
	}
	
	//this method receives type of an object and the value as strings and returns this object
	// eg. received "java.lang.Integer" and "100" return int 100
	public static Object stringToObject(String type, String value)
	{
		Class<?> c = null;
		Object object = null;
		try {
			c = Class.forName(type);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Constructor<?> cons = null;
		try {
			cons = c.getConstructor(String.class);
		} catch (NoSuchMethodException | SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			 object = cons.newInstance(value);
		} catch (InstantiationException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return object;
	
	}
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
		Class c = Class.forName("java.lang.Integer");
		System.out.println(c.getClass()+"");
	}
	
	
}
