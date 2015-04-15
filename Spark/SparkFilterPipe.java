import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class SparkFilterPipe extends Pipe {
	
	Function func;
	
	public SparkFilterPipe() {
		func = new Function<HashMap<String, String>, Boolean>() {
			public Boolean call(HashMap<String, String> h)
					throws Exception {
				if (Integer.parseInt(h.get("PageRank")) > 100) {
					return true;
				} else {
					return false;
				}
			}
		};
	}

	@Override
	public Object execute() {
		if (this.pre.size() == 0) return null;
		return ((JavaRDD) this.pre.get(0).execute()).filter(func);
	}
}
