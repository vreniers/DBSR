package dbsr.cost.values;

import java.util.LinkedList;

/**
 * Combines multiple JoinQueries, highlights the cost of individual querys.
 * 
 * Reflects the cost of a complete query plan over MULTIPLE CANDIDATES.
 * 
 * @author vincent
 *
 */
public class CostValueMultiJoinQuery extends CostValueJoinQuery<CostValueSingleJoinQuery> {

	public CostValueMultiJoinQuery(LinkedList<CostValueSingleJoinQuery> joinQueries) {
		super(joinQueries);
	}
	
	public CostValueMultiJoinQuery(CostValueSingleJoinQuery joinQuery) {
		super(joinQuery);
	}
	
	public LinkedList<CostValueSingleJoinQuery> getJoinQueries() {
		return new LinkedList<CostValueSingleJoinQuery>(getQueryCosts());
	}
	
	/**
	 * Creates a new value cost including the given query.
	 * 
	 * Assuming the given query has a relationship with the last query of the join query.
	 * 
	 * @param qry
	 * @return
	 */
	public static CostValueMultiJoinQuery merge(CostValueSingleJoinQuery joinQuery, CostValueQuery qry) {
		LinkedList<CostValueQuery> qryList = new LinkedList<CostValueQuery>();
		qryList.add(qry);
		
		CostValueSingleJoinQuery singleJoinQuery = new CostValueSingleJoinQuery(joinQuery.getLastQueryFrequency(), qryList);
		
		LinkedList<CostValueSingleJoinQuery> multiJoinQueries = new LinkedList<CostValueSingleJoinQuery>();
		multiJoinQueries.add(joinQuery);
		multiJoinQueries.add(singleJoinQuery);
		
		return new CostValueMultiJoinQuery(multiJoinQueries);
	}
	
	/**
	 * Merges join query with this query sequence.
	 * 
	 * @param joinQuery
	 * @return
	 */
	public static CostValueMultiJoinQuery mergeJoins(CostValueSingleJoinQuery joinQuery, CostValueSingleJoinQuery otherJoinQuery) {
		LinkedList<CostValueSingleJoinQuery> multiJoinQueries = new LinkedList<CostValueSingleJoinQuery>();
		multiJoinQueries.add(joinQuery);
		multiJoinQueries.add(otherJoinQuery);
		
		return new CostValueMultiJoinQuery(multiJoinQueries);
	}
	
	/**
	 * Merges join query with this query sequence.
	 * 
	 * @param joinQuery
	 * @return
	 */
	public static CostValueMultiJoinQuery mergeJoins(CostValueMultiJoinQuery joinQuery, CostValueSingleJoinQuery otherJoinQuery) {
		LinkedList<CostValueSingleJoinQuery> multiJoinQueries = new LinkedList<CostValueSingleJoinQuery>();
		multiJoinQueries.addAll(joinQuery.getJoinQueries());
		multiJoinQueries.add(otherJoinQuery);
		
		return new CostValueMultiJoinQuery(multiJoinQueries);
	}
	
	/**
	 * Merges join query with this query sequence.
	 * 
	 * @param joinQuery
	 * @return
	 */
	public static CostValueMultiJoinQuery mergeJoins(CostValueMultiJoinQuery joinQuery, CostValueMultiJoinQuery otherJoinQuery) {
		LinkedList<CostValueSingleJoinQuery> multiJoinQueries = new LinkedList<CostValueSingleJoinQuery>();
		multiJoinQueries.addAll(joinQuery.getJoinQueries());
		multiJoinQueries.addAll(otherJoinQuery.getJoinQueries());
		
		return new CostValueMultiJoinQuery(multiJoinQueries);
	}


	// TODO fix this; now its the same as a SingleJoinQuery.
	// Frequency of Previous one. Times next; If it does not start with Sec index???  => Which it doesnt if Top Freq = 1 ... Unlesss... 
	/**
	 * Multiple cases:
	 * 1) [Users] -> [Items|Bids]
	 * 		Cardinality between Items and Users.
	 * 
	 * 2) [Users|Items] -> [Items|Bids]
	 * 		Cardinality between Items and Users. Even though we dont query Items at [Items|Bids] (potentially).
	 * 		This should reflect in the secondary index not existing at [Items|Bids] due to it being shifted up by the Query Plan realizing this.
	 * 		E.g. if the first query isn't allowed at the top. We need to find out the next query.
	 * 
	 */
	@Override
	public int getCost() {
		int cost = 0;
		
		for(CostValueSingleJoinQuery singleQueryCost: getQueryCosts()) {
			int queryCost = singleQueryCost.getCost();
			
			cost += queryCost;
			// Sum E.g. [Users|Items] -> [Items|Bids] => Select + Select. 
		}
		
		return cost;
	}
}
