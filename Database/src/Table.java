import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

public class Table implements Serializable {
	public static int maxPageNumber;
	public static ArrayList<Vector> pages; 
	public static String name;
	public Table(String name) {
		this.name=name;
		maxPageNumber=0;
		pages = new ArrayList();
		// TODO Auto-generated constructor stub
	}
	
}
