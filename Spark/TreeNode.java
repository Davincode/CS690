import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONObject;


public class TreeNode {

	private List<TreeNode> parents;
	private int type;
	private String path;
	private Function func;
	private JSONObject description;

	
	public TreeNode(int type, Function function)
	{
		this.parents = new ArrayList<TreeNode>();
		this.type = type;
		this.func = function;
	}
	
	public TreeNode(int type, String path)
	{
		this.parents = new ArrayList<TreeNode>();
		this.type = type;
		this.path = path;
	}
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Function getFunc() {
		return func;
	}

	public void setFunc(Function func) {
		this.func = func;
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
	
	// serialize
	// janino expression
}
