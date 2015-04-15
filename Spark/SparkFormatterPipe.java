import java.util.HashMap;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class SparkFormatterPipe extends Pipe {
	
	Function func;
	public static String[] fields;

	public SparkFormatterPipe(final String[] columns) {
		this.func = new Function<String, HashMap<String, String>>() {
			public HashMap<String, String> call(String line) throws Exception {
				String[] str = line.split(",");
				HashMap<String, String> record = new HashMap<String, String>();
				for (int i = 0; i < columns.length; i++) {
					record.put(columns[i], str[i]);
				}
				return record;
			}
		};
	}

	@Override
	public Object execute() {
		if (this.pre.size() == 0) return null;
		return ((JavaRDD<String>) this.pre.get(0).execute()).map(func);
	}
}
