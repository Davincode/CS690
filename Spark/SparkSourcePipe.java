import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkSourcePipe extends Pipe {
	
	private String path;

	public SparkSourcePipe(String path) {
		this.path = path;
	}

	@Override
	public Object execute() {
		SparkConf conf = new SparkConf().setAppName("Java_Spark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> line = sc.textFile(path);
		return line;
	}
}
