import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class Substring extends BaseOperation implements Function {

	private int X;
	
	public Substring(int length)
	{
		this.X = length;
	}
	
	public void operate( FlowProcess flowProcess, FunctionCall functionCall )
    {
	    // get the arguments TupleEntry
	    TupleEntry arguments = functionCall.getArguments();
	
	    // create a Tuple to hold our result values
	    Tuple result = new Tuple();
	
	    // insert some values into the result Tuple
	    if(arguments.size() > 0)
	    {
		    String sourceIP = arguments.getString(0);
		    int length = sourceIP.length();
		    if(X > length)
		    {
		    	result.add((Object)sourceIP.substring(1, length));
		    }
		    else
		    {
		    	result.add((Object)sourceIP.substring(1, X));
		    }
		    for(int i = 1; i < arguments.size(); i++)
		    {
		    	result.add(arguments.getObject(i));
		    }
	    }
	    
	    // return the result Tuple
	    functionCall.getOutputCollector().add( result );
    }
}
