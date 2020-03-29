import java.io.Serializable;
import java.util.Hashtable;
import java.util.Set;

public class Tuple implements Serializable{

	public Hashtable<String, Object> data;
	public Tuple(Hashtable<String, Object> data) {
		this.data=data;
		// TODO Auto-generated constructor stub
	}
	public void view() {
		// TODO Auto-generated method stub
		Set<String> set = data.keySet();
		for(String key : set)
		{
			System.out.print(key+": "+data.get(key)+" ");
		}
		System.out.println();
		
	}
}
