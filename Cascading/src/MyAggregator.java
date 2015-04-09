import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class MyAggregator extends BaseOperation<MyAggregator.Context>
    implements Aggregator<MyAggregator.Context> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static class Context
    {
		String IP = new String();
		float value = 0;
    }

	public MyAggregator(int num_args)
    {
	    // expects n arguments, fail otherwise
	    super( num_args, new Fields( "IP", "sum" ) );
    }

	public MyAggregator( int num_args, Fields fieldDeclaration )
    {
	    // expects n arguments, fail otherwise
	    super( num_args, fieldDeclaration );
    }

	public void start( FlowProcess flowProcess,
                     AggregatorCall<Context> aggregatorCall )
    {
	    // set the context object, starting at zero
	    aggregatorCall.setContext( new Context() );
    }

	public void aggregate( FlowProcess flowProcess,
                         AggregatorCall<Context> aggregatorCall )
    {
	    TupleEntry arguments = aggregatorCall.getArguments();
	    Context context = aggregatorCall.getContext();

	    // add the current argument value to the current sum
	    context.IP = (String)arguments.getObject( 0 );
	    context.value += Float.parseFloat((String)arguments.getObject( 1 ));
    }

	public void complete( FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall )
    {
	    Context context = aggregatorCall.getContext();
	
	    // create a Tuple to hold our result values
	    Tuple result = new Tuple();
	
	    // set the sum
	    result.add( context.IP );
	    result.add( context.value );
	
	    // return the result Tuple
	    aggregatorCall.getOutputCollector().add( result );
    }
}