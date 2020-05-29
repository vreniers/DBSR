package dbsr.cost.values;

public class CostConstants {
	
	/**
	 * In ms, can be used to initialize a single query command cost.
	 */
	public static final int LATENCY = 2000;
	
	/**
	 * Cost of executing a single / subsequent query. 
	 * (Can be made dependent on the latency)
	 */
	public static final int SINGLE_QUERY_COST = 500;
	
	/**
	 * Cost of retrieving a single record (may be horizontal partitioned.)
	 * TODO: Not used yet, though some form of record cost exist simply by using the field costs multiplied.
	 */
	public static final int QUERY_RECORD_COST = 3;
	
	/**
	 * Cost of a single field. May vary more wildly in practice due to size.
	 */
	public static final int FIELD_COST = 1;
}
