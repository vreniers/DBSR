package dbsr.cost.values;

import java.util.Collections;
import java.util.LinkedList;

import dbsr.workload.query.Query;

/**
 * Mainly used to identify how many selects occurred for a given query, at a point in time.
 * 
 * @author vincent
 *
 */
public abstract class CostValueJoinQuery<T extends CostValue> extends CostValue {
	
	private final LinkedList<T> queryCosts;
	
	private final int frequency;
	
	private final int recordsSelected;
	
	public CostValueJoinQuery(LinkedList<T> queryCosts) {
		this(1, queryCosts);
	}
	
	public CostValueJoinQuery(int frequency, LinkedList<T> queryCosts) {
		this(frequency, 1, queryCosts);
	}
	
	public CostValueJoinQuery(int frequency, int recordsSelected, LinkedList<T> queryCosts) {
		this.frequency = frequency;
		this.recordsSelected = recordsSelected;
		this.queryCosts = new LinkedList<T>(queryCosts);
		
	}
	
	public CostValueJoinQuery(T qry) {
		this.queryCosts = new LinkedList<T>();
		this.frequency = 1;
		this.recordsSelected = 1;
		
		queryCosts.add(qry);
	}

	public int getFrequency() {
		return this.frequency;
	}
	
	public int getRecordsSelected() {
		return this.recordsSelected;
	}
	
	public LinkedList<T> getQueryCosts() {
		return new LinkedList<T>(queryCosts);
	}
	
	/**
	 * Adds the cost of a new query in the path.
	 * 
	 * This query is the cost of the query executed after all existing queries.
	 * 
	 * @param queryCost
	 */
	public void addQueryCost(T queryCost) {
		this.queryCosts.add(queryCost);
	}
	
	@Override
	public abstract int getCost();
}
