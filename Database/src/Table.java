import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
	public static int maxPageNumber;
	public static Vector<String> pageFiles; 
	public static String name;
	public Table(String name) {
		this.name=name;
		maxPageNumber=0;
		pageFiles = new Vector<>();
		// TODO Auto-generated constructor stub
	}
	
}
