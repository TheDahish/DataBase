import java.io.BufferedReader;
import java.io.File;
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
				{
					csvWriter.append("True");
					newTable.clusterkey=strClusteringKeyColumn;
				}
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
					System.out.println("wrong type, expected: "+type+" Received "+valueType);
					
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
					Object clusterKey = htblColNameValue.get(tempTable.clusterkey);
					String clusterType = clusterKey.getClass()+"";
				
					boolean added=false;
					switch(clusterType)
					{
					case "java.lang.Integer":
						insertIntegerOrDouble(tempTable.pageFiles,htblColNameValue,tempTable.clusterkey, tempTable);
						break;
					case "java.lang.Double":
						insertIntegerOrDouble(tempTable.pageFiles,htblColNameValue,tempTable.clusterkey, tempTable);
						break;
					case "java.lang.String":
						insertString(tempTable.pageFiles,htblColNameValue,tempTable.clusterkey, tempTable);
						break;
					}
						
				}
				
				
			}
			else {
				throw new DBAppException("dsds");
			}
		}
		
	}
	private void insertString(Vector<String> pageFiles, Hashtable<String, Object> htblColNameValue, String clusterkey,
			Table tempTable) {
		// TODO Auto-generated method stub
		Tuple newTuple = new Tuple(htblColNameValue);
		for(int j =0; j<pageFiles.size();j++)
		{
			Vector<Tuple> page = readPage(pageFiles.get(j));
			if(((String)page.get(page.size()-1).data.get(clusterkey)).compareTo((String)htblColNameValue.get(clusterkey))>0)
			{
				int i;
				for(i = page.size()-1 ;(i>=0 && ((String)page.get(page.size()-1).data.get(clusterkey)).compareTo((String)htblColNameValue.get(clusterkey))>0);i--)
				{
					page.add(i+1, page.get(i));
				}
				page.add(i+1, newTuple);
				storePageWithPath(page, pageFiles.get(j));
			//	return true;
				
			}
			else
				if(((String)page.get(page.size()-1).data.get(clusterkey)).compareTo((String)htblColNameValue.get(clusterkey))<0&&j==pageFiles.size()-1)
				{
					page.add(newTuple);
					storePageWithPath(page, pageFiles.get(j));
				}
			sortPages(pageFiles,tempTable);
			
		}
		
		
	}



	private void insertIntegerOrDouble(Vector<String> pageFiles, Hashtable<String, Object> htblColNameValue,
			String clusterKey, Table table) {
		Tuple newTuple = new Tuple(htblColNameValue);
		for(int j =0; j<pageFiles.size();j++)
		{
			Vector<Tuple> page = readPage(pageFiles.get(j));
			if(((double)page.get(page.size()-1).data.get(clusterKey))> ((double)htblColNameValue.get(clusterKey)))
			{
				int i;
				for(i = page.size()-1 ;(i>=0 && ((double)page.get(i).data.get(clusterKey)>(double)htblColNameValue.get(clusterKey)));i--)
				{
					page.add(i+1, page.get(i));
				}
				page.add(i+1, newTuple);
				storePageWithPath(page, pageFiles.get(j));
			//	return true;
				
			}
			else
				if(((double)page.get(page.size()-1).data.get(clusterKey))< ((double)htblColNameValue.get(clusterKey))&&j==pageFiles.size()-1)
				{
					page.add(newTuple);
					storePageWithPath(page, pageFiles.get(j));
				}
			sortPages(pageFiles,table);
			
		}
		
	}



	private void sortPages(Vector<String> pageFiles,Table table) {
		// TODO Auto-generated method stub
		for(int i = 0 ; i<pageFiles.size()-1;i++)
		{
			Vector<Tuple> page = readPage(pageFiles.get(i));
			Vector<Tuple> page2 = readPage(pageFiles.get(i+1));
			
			if(page.size()>maxPageSize)
			{
				page2.add(0, page.get(page.size()-1));
				page.remove(page.size()-1);
				storePageWithPath(page, pageFiles.get(i));
				storePageWithPath(page2, pageFiles.get(i+1));
				
			}
			
		}
		Vector<Tuple> page = readPage(pageFiles.get(pageFiles.size()-1));
		if(page.size()>maxPageSize)
		{
			Vector<Tuple> newPage = new Vector<>();
			newPage.add(page.get(page.size()-1));
			page.remove(page.size()-1);
			storePageWithPath(page, pageFiles.get(pageFiles.size()-1));
			storePage(table, newPage);
		}
		
	}



	public void updateTable(String strTableName,
			 String strClusteringKey,
			Hashtable<String,Object> htblColNameValue)
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
			
			Set<String> keys = htblColNameValue.keySet();
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
			
			boolean updated = false;

			BufferedReader br = new BufferedReader(new FileReader("./data/metadata.csv"));
			String line = br.readLine();
			while(line!=null)
			{
				String [] strdata = line.split(",");
				String name= strdata[0];
				String attrname = strdata[1];
				String attrvalue = strdata[2];
				String type = strdata[3];
				if(name.equals(strTableName) && type.equals("True"))
				{
					outerloop:
					for(String path: tempTable.pageFiles)
					{			
						Vector<Tuple> loadedPage = readPage(path);
						for(Tuple tuple : loadedPage) {							
							if(tuple.data.containsKey(attrname) && tuple.data.get(attrname).equals(strClusteringKey)) 
							{
								Set<String> keys2 = tuple.data.keySet();
								for(String key:keys)
								{
									for(String key2:keys2)
									{
										if(key.equals(key2))
										{
											tuple.data.put(key2, htblColNameValue.get(key));
											break;
										}
									}
								}
								storePageWithPath(loadedPage, path);
								updated= true;
								break outerloop;
							}	
						}
						
					}
				}
				
				
				
				line=br.readLine();
			}
			br.close();
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
						deletepage(tempTable,path);
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
	private void deletepage(Table tempTable, String path) {
		File f = new File (path);
		 if(f.delete()) 
	        { 
	            System.out.println("File deleted successfully"); 
	        } 
	        else
	        { 
	            System.out.println("Failed to delete the file"); 
	        } 
		 
		 tempTable.pageFiles.remove(path);
		// TODO Auto-generated method stub
		
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
	public static void storePageWithPath(Vector<Tuple> page,String path)
	{
		try {
	         FileOutputStream fileOut =
	         new FileOutputStream(path);
	         ObjectOutputStream out = new ObjectOutputStream(fileOut);
	         out.writeObject(page);
	         out.close();
	         fileOut.close();
	        // System.out.printf("success");
	      } catch (IOException i) {
	         i.printStackTrace();
	      }
	}
	 public static int stringCompare(String str1, String str2) 
	    { 
	  
	        int l1 = str1.length(); 
	        int l2 = str2.length(); 
	        int lmin = Math.min(l1, l2); 
	  
	        for (int i = 0; i < lmin; i++) { 
	            int str1_ch = (int)str1.charAt(i); 
	            int str2_ch = (int)str2.charAt(i); 
	  
	            if (str1_ch != str2_ch) { 
	                return str1_ch - str2_ch; 
	            } 
	        } 
	  
	        // Edge case for strings like 
	        // String 1="Geeks" and String 2="Geeksforgeeks" 
	        if (l1 != l2) { 
	            return l1 - l2; 
	        } 
	  
	        // If none of the above conditions is true, 
	        // it implies both the strings are equal 
	        else { 
	            return 0; 
	        } 
	    } 
	public static void main(String[] args) throws DBAppException, IOException, ClassNotFoundException {
		Object o = new Integer(5);
		Object o2 = new String("ede");
		Object o3 =  new Double(4);
	
		
		System.out.println("sdsd".compareTo("zssw"));
	}
	
	
}
