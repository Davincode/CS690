import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class SparkFilterPipe extends Pipe {
	
	Function func;
	
	public SparkFilterPipe(Function f) {
		func = f;
	}

	@Override
	public Object execute() {
		if (this.pre.size() == 0) return null;
		return ((JavaRDD) this.pre.get(0).execute()).filter(func);
	}
}
