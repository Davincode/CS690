import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class MyFilter extends BaseOperation implements Filter {

	private int limit;
	
	public MyFilter(int limit)
	{
		this.limit = limit;
	}
	
	public boolean isRemove( FlowProcess flowProcess, FilterCall call )
    {
	    // get the arguments TupleEntry
	    TupleEntry arguments = call.getArguments();
	    
	    // test the argument values and set isRemove accordingly
	    String ranking = (String)arguments.getObject(1);
	    return Integer.parseInt(ranking) < this.limit;
    }
}
