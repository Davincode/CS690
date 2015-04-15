import java.util.HashMap;


public class Builder {

	private HashMap<TreeNode, Pipe> map;
	private TreeNode root;
	
	public Builder(TreeNode root)
	{
		this.map = new HashMap<TreeNode, Pipe>();
		this.root = root;
	}
	
	public void create(TreeNode root)
	{
		if(root == null) return;
		
		Pipe pipe = null;
		if(root.getType() == 1)
		{
			pipe = new SparkFilterPipe();
		}
		else if(root.getType() == 2)
		{
			pipe = new SparkFormatterPipe(root.getColumns());
		}
		else if(root.getType() == 3)
		{
			pipe = new SparkSourcePipe(root.getPath());
		}
		
		map.put(root, pipe);
		for( TreeNode node : root.getParents())
		{
			create(node);
		}
	}
	
	public void connect(TreeNode root)
	{
		if(root == null) return;
		for(TreeNode p : root.getParents())
		{
			Pipe pre = map.get(p);
			map.get(root).addPre(pre);
		}
		for(TreeNode p : root.getParents())
		{
			connect(p);
		}
	}
	
	public Object execute()
	{
		return map.get(root).execute();
	}
}
