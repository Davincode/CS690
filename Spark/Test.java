import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class Test {

	public static void main(String[] args)
	{
		final String[] columns = new String[] { "PageURL", "PageRank", "AvgDuration" };
		final String path = "/Users/luchao/Downloads/amplab/text/tiny/rankings/part";
		
		// build a test tree
		TreeNode root = new TreeNode(1);
		TreeNode pre = new TreeNode(2, columns);
		root.addParent(pre);
		TreeNode t = new TreeNode(3, path);
		pre.addParent(t);
		
		// Graph to spark execution
		Builder b = new Builder(root);
		b.create(root);
		b.connect(root);
		JavaRDD<HashMap<String, String>> result = (JavaRDD<HashMap<String, String>>)b.execute();
		
		System.out.println("Here are some examples:");
		for (HashMap<String, String> l : result.take(10)) {
			System.out.println(l.get("PageURL") + " " + l.get("PageRank") + " "
					+ l.get("AvgDuration"));
		}
	}
	
}
