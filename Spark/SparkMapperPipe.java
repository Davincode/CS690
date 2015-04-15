import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

public class SparkMapperPipe extends Pipe {
	Pipe priorPipe;
	Function func;
	JavaRDD rdd;

	public SparkMapperPipe(Pipe pipe, Function f) {
		this.priorPipe = pipe;
		this.func = f;
		this.rdd = null;
	}

	@Override
	public Object execute() {
		// TODO Auto-generated method stub
		return ((JavaRDD) priorPipe.execute()).map(func);
	}
}
