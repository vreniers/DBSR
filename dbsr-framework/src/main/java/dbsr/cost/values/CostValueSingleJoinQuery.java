package dbsr.cost.values;

import java.util.Collections;
import java.util.LinkedList;

import dbsr.workload.query.Query;

/**
 * Of a single join query on a SINGLE CANDIDATE in a query plan.
 * 	
 * @author vincent
 *
 */
public class CostValueSingleJoinQuery extends CostValueJoinQuery<CostValueQuery> {

	/**
	 * Frequency is how many times this query is executed. (E.g. due to a preceeding join).
	 * 
	 * @param frequency
	 * @param queries
	 */
	public CostValueSingleJoinQuery(int frequency, int recordsSelected, LinkedList<CostValueQuery> queries) {
		super(frequency, recordsSelected, queries);
	}
	
	/**
	 * Frequency is how many times this query is executed. (E.g. due to a preceeding join).
	 * 
	 * @param frequency
	 * @param queries
	 */
	public CostValueSingleJoinQuery(int frequency, LinkedList<CostValueQuery> queries) {
		this(frequency, 1, queries);
	}
	
	public Query getLastQuery() {
		return getQueryCosts().peekLast().getQuery();
	}
	
	public Query getFirstQuery() {
		return getQueryCosts().peekFirst().getQuery();
	}
	
	public int getLastQueryFrequency() {
		return getQueryCosts().peekLast().getSelectFrequency();
	}
	
	/**
	 * Returns list of executed queries in order.
	 * 
	 * @return
	 */
	public LinkedList<Query> getQueryList() {
		LinkedList<Query> queries = new LinkedList<Query>();
		
		for(CostValueQuery qry: getQueryCosts()) {
			queries.add(qry.getQuery());
		}
		
		return queries;
	}
	
	@Override
	public int getCost() {
		int costOfQueryOperation = CostConstants.SINGLE_QUERY_COST * getFrequency();
		int costOfSingleRecord = 0;
		
		for(CostValueQuery singleQueryCost: getQueryCosts()) {
			costOfSingleRecord += singleQueryCost.getCost();
		}		
		
//		System.out.println("freq:" + getFrequency());
//		System.out.println("records: " + getRecordsSelected());
//		System.out.println("cost single record:" + costOfSingleRecord);
		
		int costOfAllRecords = costOfSingleRecord * getRecordsSelected() * getFrequency();
		
		return costOfQueryOperation + costOfAllRecords;
	}
	
	
}
