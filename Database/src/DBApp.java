import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

//@SuppressWarnings({ "unchecked", "unchecked" })
public class DBApp {
	
	private Vector<Table> tableVector =  new Vector<>(); //contains all table objects
	public static int currentpages = 0;
	public static int nodeSize;
	public static int maxPageSize;
	
	//At the start of the program run this method to read all the tables from the disk and assign the vector to table vector
	// and read the config file
	public void init() {
			readConfig();
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

			
			Vector<Tuple> page = new Vector<>();
			Set<String> keys = htblColNameValue.keySet();
			Tuple t = null;
			String data;
			Object value;
			boolean correctTuple = true;
			for(String key:keys) {
				data = key;
				String type ="class " + checkDataType(strTableName, key);
				value=htblColNameValue.get(key);
				String valueType = value.getClass()+"";
				if(!(type.equals(valueType)))
				{
					correctTuple=false;
					
				}
				
				
				
			}
			if(correctTuple) {
			if(tempTable.maxPageNumber==0)
			{
				//first insertion in table
				
					tempTable.maxPageNumber++;
					t = new Tuple(htblColNameValue);
					page.add(t);
					storePage(tempTable, page);
					tempTable.pageFiles.add("./data/"+tempTable.name+tempTable.maxPageNumber+".class");
				
				
				
			}
			else {
				
					boolean added=false;
					for(String path: tempTable.pageFiles)
					{
						Vector<Tuple> loadedPage = readPage(path);
						if(loadedPage.size()!=maxPageSize)
						{
							t = new Tuple(htblColNameValue);
							loadedPage.add(t);
							storePage(tempTable, loadedPage);
							added=true;
							break;
							
						}
					}
					if(!added)
					{
						tempTable.maxPageNumber++;
						t = new Tuple(htblColNameValue);
						page.add(t);
						storePage(tempTable, page);
						tempTable.pageFiles.add("./data/"+tempTable.name+tempTable.maxPageNumber+".class");
					} 
				}
				
				
			}
			else {
				throw new DBAppException("dsds");
			}
		}
		
	}
	public void updateTable(String strTableName,
			 String strClusteringKey,
			Hashtable<String,Object> htblColNameValue,Hashtable<String,Object> newTuple  )
			throws DBAppException, IOException {

		Table tempTable = null;
		for(Table table : tableVector)
		{
			if(table.name.equals(strTableName))
				tempTable=table;
		}
		if(tempTable==null)
			throw new DBAppException("Table not found.");
		else {
			
			Set<String> keys = newTuple.keySet();
			String data;
			Object value;
			boolean correctTuple = true;
			for(String key:keys) {
				data = key;
				String type ="class " + checkDataType(strTableName, key);
				value=newTuple.get(key);
				String valueType = value.getClass()+"";
				if(!(type.equals(valueType)))
				{
					correctTuple=false;
					
				}
				
				
				
			}
			if(correctTuple) {
			
			boolean updated = false;
			outerloop:
			for(String path: tempTable.pageFiles)
			{			
				Vector<Tuple> loadedPage = readPage(path);
				for(Tuple tuple : loadedPage) {
					if(htblColNameValue.equals(tuple.data)) {
						tuple.data = newTuple;
						storePage(tempTable, loadedPage);
						updated= true;
						break outerloop;
					}	
				}
				
			}
			if (!updated) {
				throw new DBAppException("Tuple not found.");
			}
			
		}
			else {
				throw new DBAppException("Type mismatch.");
			}
			}
		
	}
	public void deleteFromTable(String strTableName,
			 Hashtable<String,Object> htblColNameValue)
			 throws DBAppException {
		Table tempTable = null;
		for(Table table : tableVector)
		{
			if(table.name.equals(strTableName))
				tempTable=table;
		}
		if(tempTable==null)
			throw new DBAppException("Table not found.");
		else {
			boolean deleted = false;
			outerloop:
			for(String path: tempTable.pageFiles)
			{			
				Vector<Tuple> loadedPage = readPage(path);
				for(Tuple tuple : loadedPage) {
					if(htblColNameValue.equals(tuple.data)) {
						loadedPage.remove(tuple);
						storePage(tempTable, loadedPage);
						deleted= true;
						break outerloop;
					}	
				}
				
			}
			if (!deleted) {
				throw new DBAppException("Tuple not found.");
			}
			
		}
		
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
	public static void readConfig()
	{
		try (InputStream input = new FileInputStream("./data/DBApp.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
           maxPageSize =Integer.parseInt(prop.getProperty("MaximumRowsCountinPage"));
           nodeSize = Integer.parseInt(prop.getProperty("NodeSize"));
        } catch (IOException ex) {
            ex.printStackTrace();
        }

	}
	
	//write into config file
	public static void writeConfig() {
		try (OutputStream output = new FileOutputStream("./config/DBApp.properties")) {

            Properties prop = new Properties();

            // set the properties value
            prop.setProperty("MaximumRowsCountinPage", "200");
            prop.setProperty("NodeSize", "15");
           // prop.setProperty("db.password", "password");

            // save properties to project root folder
            prop.store(output, null);

          //  System.out.println(prop);

        } catch (IOException io) {
            io.printStackTrace();
        }
	}
	
	public static Vector<Tuple> readPage(String path)
	{
		Vector<Tuple> page = null;
	      try {
	         FileInputStream fileIn = new FileInputStream(path);
	         ObjectInputStream in = new ObjectInputStream(fileIn);
	         page = (Vector<Tuple>) in.readObject();
	         in.close();
	         fileIn.close();
	      } catch (IOException i) {
	         i.printStackTrace();
	       //  break;
	      } catch (ClassNotFoundException c) {
	        // System.out.println("Employee class not found");
	         c.printStackTrace();
	       //  break;
	      }
	      return page;
	}
	
	
	public static void storePage(Table table, Vector<Tuple> page)
	{
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream("./data/"+table.name+table.maxPageNumber+".class");
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(page);
	         out.close();
	         fileOut.close();
	        // System.out.printf("success");
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
		Class c = Class.forName("java.lang.Integer");
		System.out.println(c.getClass()+"");
	}
	
	
}
