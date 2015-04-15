import java.util.ArrayList;
import java.util.List;


public abstract class Pipe{
	
	protected List<Pipe> pre;
	
	public Pipe()
	{
		this.pre = new ArrayList<Pipe>();
	}
	
	public void addPre(Pipe pre)
	{
		this.pre.add(pre);
	}
	
	public abstract Object execute();
}
