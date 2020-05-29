package dbsr.cost.values;

import dbsr.workload.query.Query;

public class CostValueQuery extends CostValue {

	/** 
	 * Select frequency, typically the frequency it appears in the overall parent record.
	 * 
	 * E.g. [users|items|bids], bids is selected it is multiplied by the number of items.
	 */
	private final int selectFrequency;
	
	private final int fieldCosts;
	
	private final Query query;
	
	public CostValueQuery(int selectFrequency, int fieldCosts, Query query) {
		this.selectFrequency = selectFrequency;
		this.fieldCosts = fieldCosts;
		this.query = query;
	}

	@Override
	public int getCost() {
		return selectFrequency * fieldCosts;
	}
	
	public Query getQuery() {
		return this.query;
	}
	
	public int getSelectFrequency() {
		return this.selectFrequency;
	}
	
	public int getFieldCosts() {
		return this.fieldCosts;
	}

}
