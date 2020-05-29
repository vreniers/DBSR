package dbsr.cost;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import dbsr.candidate.Candidate;
import dbsr.candidate.CandidateSequences;
import dbsr.cost.values.CostValueJoinQuery;
import dbsr.cost.values.CostValueMultiJoinQuery;
import dbsr.cost.values.CostValueQuery;
import dbsr.cost.values.CostValueSingleJoinQuery;
import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.query.Query;

/**
 * Calculates the cost for a query plan with candidate sequences.
 * 
 * Cost is calculated based on:
 * - Data size of the records returned
 * - Number of queries executed at each step.
 * 
 * TODO: Make static?
 * 
 * @author vincent
 *
 */
public class CostCalculatorPlan<T extends Candidate<T>> implements Cost {
	
	private final QueryPlan<T> qp;
	
	public CostCalculatorPlan(QueryPlan<T> qp) {
		this.qp = qp;
	}
	
	/**
	 * Cost of a query plan depends on results retrieved:
	 * 
	 * 1.1 number of results gathered at each step multiplied.
	 * 		e.g. Users (=1) -> Bids (10 avg. cardinality) ~ Items (1) = 1 * 10 * 1.
	 * 1.2 Secondary index at a step.
	 * 		e.g. Users (=1) -> [Item|Bids] (10 bids per item). (3 bids per user on avg). Means we get 1*3(10 bids worst-case).
	 * 
	 * 2. At each step times the cost of the data structure (e.g. Nr of fields selected).
	 */
	@Override
	public int getCost() {

		HashMap<Integer, LinkedList<Query>> mapping = this.qp.getMapping();
		HashMap<Integer, EntityTree> secondaryIndexes = this.qp.getSecondaryIndexes();
		LinkedList<T> candidates = this.qp.getCandidatesPlan();
		
		// Cost also times the frequency the previous relationship contains this next query.
		// + Shift query counter.
		CostValueMultiJoinQuery cost = new CostValueMultiJoinQuery(new LinkedList<CostValueSingleJoinQuery>());
		
		for(Integer candidateIndex: mapping.keySet()) {
			LinkedList<Query> queries = new LinkedList<Query>(mapping.get(candidateIndex));
			EntityTree candidate = candidates.get(candidateIndex.intValue()).getCandidate();
			
			cost = CostValueMultiJoinQuery.mergeJoins(cost, getCostAtStep(secondaryIndexes, candidate, candidateIndex, queries));
		}
		
		return cost.getCost();
	}

	/**
	 * Get cost at a single data structure in the linked list, and all the queries which are executed at this location.
	 * 
	 * TODO Concept of Records selected.
	 * 
	 * TODO: w is select frequency again.
	 * 
	 * Cases:
	 * - Index:
	 * 		(A): Reversed: 
	 * 			 T
	 * 
	 * @return
	 */
	private CostValueSingleJoinQuery getCostAtStep(HashMap<Integer, EntityTree> secondaryIndexes, EntityTree candidate, int candidateIndex, LinkedList<Query> queries) {
		
		LinkedList<CostValueQuery> queryCosts = new LinkedList<CostValueQuery>();
		
		candidate = candidate.clone();
		candidate.setParent(null); // redundant(?)
		
		boolean reversedPath = false;
		
		// Secondary Index present.
		// Case A: Reversed
		
		// Example: Query Items -> Bids  in [ Bids | Items].
		// First query Items, then after Bids.
		// Example: Query Users -> Items in [ Bids | Items | Users].
		// First query users, items are retrieved automatically.
		// Items have on average 10 Bids. 
		// Users sell on average 5 Items. => User appears in 50 bid records.
		if(queries.size() == 2 && secondaryIndexes.containsKey(candidateIndex) &&
				this.qp.isPathReversedAtCandidateIndex(candidateIndex)) {			
			EntityTree secondaryIndex = secondaryIndexes.get(new Integer(candidateIndex));
			
			reversedPath = true;
		}
	
		// Queries can be calculated in follow-up. (Unless query path size = 2, the index can be reversed).
		// Calculate the cost as frequency of the record times query cost.
		//
		// Possible Index Case A: Not-reversed.
		// 1.A) Users(1)-> Items(4) -> (10)Bids
		//      Results in 1 query per Item.
		// 2.A) Users(3)-> Items(4) -> (10)Bids
		// ...
		
		// TODO review ; multiple EndNodes possible (changed it just choosing it one)
		// Start at bottom node, then work upwards towards first query.
		// TODO: Either SETTINGS: TO EXPLORE MULTIPLE PATHS FOR MORE ACCURATE COST (OPTIMAL SELECTION)
		// TODO: GLOBAL SETTINGS - DEPENDING ON IF THE DOCUMENT STORE CAN QUERY UNTIL A DEPTH, OR IF IT PULLS THE ENTIRE NESTED DOCUMENT....
		// If secondary index -> End node = Secondary index?
		EntityTree endNode;
		
		if(secondaryIndexes.containsKey(candidateIndex)) 
			endNode = secondaryIndexes.get(candidateIndex);
		else
			endNode = candidate.getEndNodesOfQueryPath(queries, 0, false).get(0);
		
		LinkedList<Query> queriesReverse = new LinkedList<Query>();
		
		
		// If reversed path then the queries are already switched around.
		if(!reversedPath)
			for(Query qry: queries)
				queriesReverse.addFirst(qry);
		else
			queriesReverse = new LinkedList<Query>(queries);
		
		// Start at end node, calculcate query selection costs and iterate up.
		boolean endNodeVisited = false;
					
		while(endNode != null && !queriesReverse.isEmpty()) {
			Query executedQuery = queriesReverse.pop();
			int fieldCosts = executedQuery.getCost();
			int cardinalityToParent = endNode.getNodeFrequency();
			
			// add all underlying query costs
			if(!endNodeVisited) {
				fieldCosts = endNode.getCost(); // similar to executedQuery.getCost() (counts field + underlying nodes).
				endNodeVisited = true;
			}
			
			CostValueQuery queryCost = new CostValueQuery(cardinalityToParent, fieldCosts, executedQuery);
			queryCosts.add(queryCost);
			
			endNode = endNode.getParent();
			
		}
		
		int frequency = qp.getSelectFrequencyBetweenCandidates(candidateIndex-1, candidateIndex);
		int recordsSelected = 1;
		
		// if index present, then the record selected is also multiplied by the general times it appear.
		if(secondaryIndexes.containsKey(candidateIndex))
			recordsSelected = secondaryIndexes.get(candidateIndex).getNodeFrequencyReverse();
		
		return new CostValueSingleJoinQuery(frequency, recordsSelected, queryCosts);
	}

	/**
	 * Identifies type of join between two candidates.
	 * 
	 * E.g. [A|B] -> [C|D]
	 * = Direct
	 * 
	 * TODO: Test
	 * TODO Unused at the moment.
	 * 
	 * @param candidateIndex
	 * @param nextCandidateIndex
	 * @return
	 */
	private ConnectionType getTypeBetween(int candidateIndex, int nextCandidateIndex) {
		HashMap<Integer, LinkedList<Query>> mapping = this.qp.getMapping();
		LinkedList<Query> prevQueries = mapping.get(new Integer(candidateIndex));
		LinkedList<Query> nextQueries = mapping.get(new Integer(nextCandidateIndex));
		
		EntityTree prevCandidate = this.qp.getCandidatesPlan().get(new Integer(candidateIndex)).getCandidate();
		EntityTree nextCandidate = this.qp.getCandidatesPlan().get(new Integer(nextCandidateIndex)).getCandidate();
		
		HashMap<Integer, EntityTree> secondaryIndexes = this.qp.getSecondaryIndexes();
		
		Query prevQuery = prevQueries.getLast();
		Query nextQuery = nextQueries.getFirst();
		
		// Direct or Sub-direct
		if(!secondaryIndexes.containsKey(new Integer(nextCandidateIndex))) {
			// Direct			
			if(nextCandidate.hasQuery(nextQuery)) 
				return ConnectionType.DIRECT;
			// Sub-direct
			else if(nextCandidate.hasQuery(prevQuery)) {
				return ConnectionType.SUB_DIRECT;
			}
		} // Indexed cases
		else {
			EntityTree nodeIndex = secondaryIndexes.get(new Integer(nextCandidateIndex));
			
			// Normal index
			if(nodeIndex.getParent().hasQuery(prevQuery) && nodeIndex.getParent().isAlwaysContainedByParent()) {
				return ConnectionType.INDEXED_SUB;
			}
			
			// Index sub
			return ConnectionType.INDEXED;
			
			// TODO Optional: finish for reversed
		}
		
		return ConnectionType.UNKNOWN;
	}
	
	/**
	 * Sums up how 2 candidates in a query plan are related.
	 * 
	 * Case: Not-indexed:
	 * - Direct      [A->B] [C->D]
	 * - Sub-direct  [A->B] [B->C->D]
	 * 
	 * Case: Indexed
	 * - Nrmal Index    [A->B] [X->C->D]
	 * - Index sub      [A->B] [X->B->C->D] 
	 * - Reversed       [A->B] [D->C]        - Don't think this case exists as of right now?
	 * - ReversedInd    [A->B] [X->D->C]
	 * - ReversedIndSub [A->B] [B->D->C]
	 * 
	 * What about
	 * 					[A->B] [X->C->Y->D]  Impossible due to merge
	 * 
	 * @author vincent
	 */
	private enum ConnectionType{
		
		/**
		 * [A->B] => [C->D] for Qry A,B,C,D
		 */
		DIRECT,
		
		/**
		 * [A->B] => [B->C->D] for Qry A,B,C,D
		 */
		SUB_DIRECT,
		
		/**
		 * [A->B] => [X->C->D] 
		 */
		INDEXED,
		
		/**
		 * [A->B] => [X->B->C->D]
		 */
		INDEXED_SUB,
		
		/**
		 * [A->B] => [D->C]  (means Indexed)
		 */
		REVERSED,
		
		/**
		 * [A->B] => [X->D->C]  (means Indexed)
		 */
		INDEXED_REVERSED,
		
		/**
		 * [A->B] => [B->D->C]  (means Indexed)
		 */
		INDEXED_REVERSED_SUB,
		
		/**
		 * Alternative 
		 */
		UNKNOWN,
	}
	
}
