import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
	private int pageNumber;
	public static Vector<String> pageFiles; 
	private String name;
	public Table(String name) {
		this.name=name;
		pageNumber=0;
		pageFiles = new Vector<>();
		// TODO Auto-generated constructor stub
	}
	
}
