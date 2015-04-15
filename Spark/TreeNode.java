import java.util.ArrayList;
import java.util.List;


public class TreeNode {

	private List<TreeNode> parents;
	private int type;
	private String path;
	private String[] columns;
	
	public TreeNode(int type)
	{
		this.parents = new ArrayList<TreeNode>();
		this.type = type;
	}
	
	public TreeNode(int type, String path)
	{
		this.parents = new ArrayList<TreeNode>();
		this.type = type;
		this.path = path;
	}
	
	public String[] getColumns() {
		return columns;
	}

	public void setColumns(String[] columns) {
		this.columns = columns;
	}

	public TreeNode(int type, String[] columns)
	{
		this.parents = new ArrayList<TreeNode>();
		this.type = type;
		this.columns = columns;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public List<TreeNode> getParents()
	{
		return parents;
	}
	
	public void addParent(TreeNode parent)
	{
		this.parents.add(parent);
	}
}
