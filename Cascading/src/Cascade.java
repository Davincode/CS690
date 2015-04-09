import java.util.Collections;
import java.util.Properties;

import org.apache.hadoop.mapred.JobConf;

import cascading.operation.filter.Limit;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.joiner.InnerJoin;


public class Cascade {
	
	public Cascade()
	{
		
	}
	
	public void scan(String inPath, String outPath, int X)
	{		
	    JobConf jobConf = new JobConf();
	    jobConf.setJarByClass( Cascade.class );
	    
	    Properties properties = AppProps.appProps()
	    		  .setName( "scan" )
	    		  .setVersion( "1.2.3" )
	    		  .buildProperties( jobConf );
	    
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
	    
		Fields input = new Fields( "pageURL", "pageRank", "avgDuration");
		
		Tap source = new Hfs(new TextDelimited( input, false, "," ), inPath );
		Tap sink = new Hfs( new TextDelimited( false, "," ), outPath  );
		
		Pipe pipe = new Pipe("scan");
		pipe = new Each(pipe, new MyFilter(X));

		Flow flow = flowConnector.connect( "flow-name", source, sink, pipe );
		flow.complete();
	}
	
	public void aggregation(String inPath, String outPath)
	{
		JobConf jobConf = new JobConf();
	    jobConf.setJarByClass( Cascade.class );
	    
	    Properties properties = AppProps.appProps()
	    		  .setName( "aggregate" )
	    		  .setVersion( "1.2.3" )
	    		  .buildProperties( jobConf );
	    
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
	    
		Fields input = new Fields( "sourceIP", "destURL", "visitDate", "adRevenue", 
	    		"userAgent", "countryCode", "languageCode", "searchWord", "duration");
		
		Tap source = new Hfs(new TextDelimited( input, false, "," ), inPath );
		Tap sink = new Hfs( new TextDelimited( false, "," ), outPath  );
		
		Pipe pipe = new Pipe("aggregation");
		pipe = new Each(pipe, new Substring(10), input);
		
		Pipe groupBy = new GroupBy( pipe , new Fields("sourceIP"));
		groupBy = new Every( groupBy, new Fields("sourceIP", "adRevenue"), new MyAggregator(2, new Fields( "IP", "sum" )), new Fields("IP", "sum"));
		
		FlowDef flowDef = FlowDef.flowDef()
			     .addSource( pipe, source )
			     .addTailSink( groupBy, sink );
			
	    // run the flow
	    flowConnector.connect( flowDef ).complete();
	}
	
	public void join(String RPath, String UVPath, String outPath, int year)
	{	
		JobConf jobConf = new JobConf();
	    jobConf.setJarByClass( Cascade.class );
	    
	    Properties properties = AppProps.appProps()
	    		  .setName( "join" )
	    		  .setVersion( "1.2.3" )
	    		  .buildProperties( jobConf );
	    
	    HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );
	    
		Fields lhsFields = new Fields( "pageURL", "pageRank", "avgDuration" );
		Fields rhsFields = new Fields( "sourceIP", "destURL", "visitDate", "adRevenue", 
	    		"userAgent", "countryCode", "languageCode", "searchWord", "duration" );
		
		Tap R = new Hfs(new TextDelimited( lhsFields, false, "," ), RPath );
		Tap UV = new Hfs(new TextDelimited( rhsFields, false, "," ), UVPath );
		Tap sink = new Hfs( new TextDelimited( false, "," ), outPath  );
		
		Pipe Rpipe = new Pipe("Rankings");
		Pipe UVpipe = new Pipe("UserVisits");
		UVpipe = new Each(UVpipe, new DateFilter(year));

		Fields declared = new Fields(
		  "pageURL", "pageRank", "avgDuration", "sourceIP", "destURL", "visitDate", "adRevenue", 
  		"userAgent", "countryCode", "languageCode", "searchWord", "duration"
		);
		
		Pipe join = new CoGroup( Rpipe, new Fields("pageURL"), UVpipe, new Fields("destURL"), declared, new InnerJoin() );
		
		Pipe groupBy = new GroupBy( join , new Fields("sourceIP"));
		groupBy = new Every( groupBy, new Fields("sourceIP", "pageRank", "adRevenue"), new AnotherAggregator(3, new Fields( "IP", "avg", "sum" )), new Fields("IP", "sum", "avg"));
		
		Fields sum = new Fields("sum");
		sum.setComparator(sum, Collections.reverseOrder());
		Pipe result = new GroupBy( groupBy , sum);
		result = new Each(result, new Limit(1));
		
		FlowDef flowDef = FlowDef.flowDef()
			     .addSource( Rpipe, R )
			     .addSource( UVpipe, UV )
			     .addTailSink( result, sink );
			
	    // run the flow
	    flowConnector.connect( flowDef ).complete();
	}
	
	public static void main(String[] args)
	{
		Cascade cas = new Cascade();
		
		String inPath = "hdfs://localhost" + "/user/luchao/amplab/text/tiny/rankings";
	    String outPath = "hdfs://localhost" + "user/luchao/amplab/result/output";
		cas.scan(inPath, outPath, 50);
		
		inPath = "hdfs://localhost" + "/user/luchao/amplab/text/tiny/uservisits";
	    outPath = "hdfs://localhost" + "user/luchao/amplab/result/output_2";
		cas.aggregation(inPath, outPath);
		
		String RPath = "hdfs://localhost" + "/user/luchao/amplab/text/tiny/rankings";
		String UVPath = "hdfs://localhost" + "/user/luchao/amplab/text/tiny/uservisits";
	    outPath = "hdfs://localhost" + "user/luchao/amplab/result/output_3";
		cas.join(RPath, UVPath, outPath, 1990);
	}
}
