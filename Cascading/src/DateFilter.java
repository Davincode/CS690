import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class DateFilter extends BaseOperation implements Filter {

	private int X;
	
	public DateFilter(int date)
	{
		this.X = date;
	}
	
	public boolean isRemove( FlowProcess flowProcess, FilterCall call )
    {
	    // get the arguments TupleEntry
	    TupleEntry arguments = call.getArguments();
	    
	    // test the argument values and set isRemove accordingly
	    String ranking = (String)arguments.getObject(2);
	    String[] tokens = ranking.split("-");
	    boolean flag = true;
	    if(tokens.length == 3)
	    {
	    	if(Integer.parseInt(tokens[0]) > 1980 && Integer.parseInt(tokens[0]) < X)
	    		flag = false;
	    }
	    return flag;
    }
}
