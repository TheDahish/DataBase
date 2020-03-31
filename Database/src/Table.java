import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
	public int maxPageNumber;
	public Vector<String> pageFiles;
	public String name;
	public String clusterkey;
	public Table(String name) {
		this.name=name;
		maxPageNumber=0;
		pageFiles = new Vector<>();
		// TODO Auto-generated constructor stub
	}
	public void view()
	{
		for(String path:pageFiles)
		{
			System.out.println(path);
			Vector<Tuple> page = readPage(path);
			for(Tuple record : page)
			{
				System.out.println(name);
				record.view();
			}
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
}
