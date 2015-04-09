import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.AggregatorCall;
import cascading.operation.BaseOperation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;


public class AnotherAggregator extends BaseOperation<AnotherAggregator.Context>
    implements Aggregator<AnotherAggregator.Context> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static class Context
    {
		String IP = new String();
		float value_1 = 0;
		float value_2 = 0;
		int num = 0;
    }

	public AnotherAggregator(int num_args)
    {
	    // expects n arguments, fail otherwise
	    super( num_args, new Fields( "IP", "avg", "sum" ) );
    }

	public AnotherAggregator( int num_args, Fields fieldDeclaration )
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
	    context.value_1 += Float.parseFloat((String)arguments.getObject( 1 ));
	    context.value_2 += Float.parseFloat((String)arguments.getObject( 2 ));
	    
	    context.num += 1;
    }

	public void complete( FlowProcess flowProcess,
                        AggregatorCall<Context> aggregatorCall )
    {
	    Context context = aggregatorCall.getContext();
	
	    // create a Tuple to hold our result values
	    Tuple result = new Tuple();
	
	    // set the sum
	    result.add( context.IP );
	    result.add( context.value_1 / context.num);
	    result.add( context.value_2);
	
	    // return the result Tuple
	    aggregatorCall.getOutputCollector().add( result );
    }
}