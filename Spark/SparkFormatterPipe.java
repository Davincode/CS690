import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class SparkFormatterPipe extends Pipe {
	
	Function func;
	public static String[] fields;

	public SparkFormatterPipe(Function func) {
		this.func = func;
	}

	@Override
	public Object execute() {
		if (this.pre.size() == 0) return null;
		return ((JavaRDD<String>) this.pre.get(0).execute()).map(func);
	}
}
