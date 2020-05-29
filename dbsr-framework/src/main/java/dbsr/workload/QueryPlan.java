package dbsr.workload;

import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import dbsr.candidate.Candidate;
import dbsr.config.Config;
import dbsr.cost.Cost;
import dbsr.cost.CostCalculatorPlan;
import dbsr.cost.Rank;
import dbsr.model.tree.EntityTree;
import dbsr.workload.query.Query;

/**
 * Elicits the traversed candidates for a sequence (=query).
 * 
 * @author root
 * 
 * TODO: Check equals works?
 * [Users] -> [Bids|items] -> [User] (was present double in TreeSet once).
 * 
 *
 */
public class QueryPlan<T extends Candidate<T>> implements Comparable<QueryPlan<T>>, Cost, Rank<Integer> {
	
	private final Sequence sequence;
	
	private final LinkedList<T> candidates;
	
	private HashMap<Integer, EntityTree> secondaryIndexes = new HashMap<Integer, EntityTree>();
	
	/**
	 * Number of previous queries that are already answered in the ET.
	 * 
	 * E.g. [Users|Bids] -> [Bids|Items]. if Bids are already queried, we dont need an SI on Items.
	 */
	private final HashMap<Integer, Integer> previousQueriesPresent = new HashMap<Integer, Integer>();
	
	/**
	 * TODO: Equivalent of secondary index. Keeps track of what query is applied at what candidate.
	 * Data may exist multiple times. E.g. select User1 and Items1 at C1.
	 * 
	 * This can result in secondary index for Items1 at C1.
	 */
	private final HashMap<Integer, LinkedList<Query>> queryDataMapping;
	
	private Integer rank;
	
	private double fitness;
	
	/**
	 * Creates a query plan for a given sequence (query).
	 * The plan is executed on certain candidate collections.
	 * 
	 * Should a collection 
	 * @param seq
	 * @param candidates
	 * @param indexes
	 */
	public QueryPlan(Sequence seq, LinkedList<T> candidates, HashMap<Integer, LinkedList<Query>> dataMapping) {
		this.sequence = seq;
		this.candidates = candidates;
		this.queryDataMapping = dataMapping;
		
		if(this.queryDataMapping.isEmpty()) {
			if(seq.getQueryPath().size() == candidates.size()) {
				for(int i=0; i < candidates.size(); i++) {
					LinkedList<Query> queries = new LinkedList<Query>();
					queries.add(seq.getQueryPath().get(i));
					
					queryDataMapping.put(new Integer(i), queries);
				}
			}
		}
		
		this.setSecondaryIndexes();
	}
	
	/**
	 * Creates a query plan for a given sequence (query).
	 * The plan is executed on certain candidate collections.
	 * 
	 * Should a collection 
	 * @param seq
	 * @param candidates
	 */
	public QueryPlan(Sequence seq, LinkedList<T> candidates) {
		this(seq, candidates, new HashMap<Integer, LinkedList<Query>>());
	}
	
	public LinkedList<T> getCandidatesPlan() {
		return this.candidates;
	}
	
	/**
	 * Gets executed queries in order.
	 * 
	 * @return
	 */
	public LinkedList<Query> getQueries() {
		int nrCandidates = queryDataMapping.size();
		LinkedList<Query> queryList = new LinkedList<Query>();
		
		for(int i=0; (i < nrCandidates && i < queryDataMapping.size()); i++) {
			queryList.addAll(queryDataMapping.get(new Integer(i)));
		}
		
		return queryList;
	}
	
	/**
	 * Returns the query that still has to be executed after the given sequence of queries.
	 * 
	 * @param queries
	 * @return
	 */
	public Query getQueryAfter(LinkedList<Query> queries) {
		LinkedList<Query> totalQueries = getQueries();
		LinkedList<Query> queriesExecuted = (LinkedList<Query>) totalQueries.subList(0, queries.size()-1);
		
		if(queriesExecuted.equals(queries) && queries.size() != totalQueries.size())
			return totalQueries.get(queries.size());
		else
			return null;
	}
	
	public Sequence getSequence() {
		return this.sequence;
	}
	
	/**
	 * Gets a list of queries already executed (in order) before a given candidate.
	 * (Used to check if we need a secondary index at some points).
	 * 
	 * @param candidateIndex
	 * @return
	 */
	public LinkedList<Query> getQueriesBeforeCandidate(int candidateIndex) {	
		if(candidateIndex < 0)
			return null;
		
		// Get queries executed before a given Candidate structure.
		LinkedList<Query> queryList = new LinkedList<Query>();
		
		for(int i=0; (i < candidateIndex && i < queryDataMapping.size()); i++) {
			queryList.addAll(queryDataMapping.get(new Integer(i)));
		}
		
		return queryList;
	}
	
	
	/**
	 * Get all select frequencies between candidates in the query plan.
	 * 
	 * @return
	 */
	public LinkedList<Integer> getSelectFrequencies() {
		LinkedList<Integer> selectFrequencies = new LinkedList<Integer>();
		
		for(Integer candidateIndex: queryDataMapping.keySet()) {
			int frequency = getSelectFrequencyBetweenCandidates(candidateIndex - 1, candidateIndex);
			
			selectFrequencies.add(frequency);
		}
		
		return selectFrequencies;
	}
	
	/**
	 * Calculates how many records are selected in the next step of a query plan.
	 * 
	 * Examples:
	 * 1) [Users|Items] -> [Items|Bids]  = 1 (if Q0: Users, Items and Q1: Bids)
	 * 2) [Users] -> [Items|Bids] = Cardinality between Items and Users. (if Q1: Items, Bids).
	 * 
	 * can also be longer:
	 * 
	 * 3) [Users|Items|Bids] -> [Items|Bids|Users]
	 * 
	 * TODO: Untested
	 * 
	 * @param i
	 * @param candidateIndex
	 * @return
	 */
	public int getSelectFrequencyBetweenCandidates(int prevCandidateIndex, int candidateIndex) {	
		if (candidateIndex < 0 || prevCandidateIndex < 0)
			return 1;
		
		LinkedList<Query> nextQueries = queryDataMapping.get(new Integer(candidateIndex));
		LinkedList<Query> prevQueries = queryDataMapping.get(new Integer(prevCandidateIndex));
		
		int queriesPresent = 0;
		
		if (this.previousQueriesPresent.containsKey(candidateIndex)) {
			queriesPresent = this.previousQueriesPresent.get(candidateIndex);
		}
		
//		System.out.println("Present queries:" + queriesPresent);
		
		// If nothing was shifted. We just return the cardinality of the last query and the next query.
		// e.g. [Users] - [Items|Bids].  E.g. 5
		if(queriesPresent == 0 ) {
			Query firstQuery = nextQueries.getFirst();
			Query lastQuery = prevQueries.getLast();
			
			return lastQuery.getEntity().getRelationshipWith(firstQuery.getEntity()).getCardinalityEntityTargeting(lastQuery.getEntity());
		} 
		else if((prevQueries.size() - queriesPresent <= 0))
		// If secondary indexes were shifted. We now compute the cardinality between the shifted query and the previous queries.
		// E.g. [Users|Items|Bids] -> [Items|Bids]. Means we select Users - Items number of records. 
		{
			return 1;		
		} else {
			Query firstQuery = prevQueries.get(prevQueries.size() - (queriesPresent));
			Query lastQuery = prevQueries.get(prevQueries.size() - (queriesPresent + 1));
			
			return lastQuery.getEntity().getRelationshipWith(firstQuery.getEntity()).getCardinalityEntityTargeting(lastQuery.getEntity());			 
		}
		
	}
	
	/**
	 * Checks if index of a candidate is in range.
	 * 
	 * @param index
	 * @return
	 */
	public boolean isValidCandidateIndex(int index) {
		if (index < 0)
			return false;
		
		if (index >= this.candidates.size())
			return false;
		
		return true;
	}
	
	/**
	 * Initializes the secondary indexes, 
	 * since this does not change after query plan creation.
	 * 
	 * Check each candidate, the relevant query path, and whether or not it occurs at the top of the candidate.
	 * 
	 * Multiple options for best secondary index are compared.
	 * 
	 * Special cases: 
	 * -	e.g. [ Users | Bids ] -> [ Bids | Items | Users] with Q0-<Users>, <Bids, Q1-<Items>; 
	 * 		We don't need secondary index at Q1 because we know which bids to select.
	 * -	Reverse index: [ Items | Bids] with Q0= Bids, Q1=Items. => No need for Q1 after Q0. Secondary Index at Bids.
	 * 		Only for query paths of SIZE 2. Size of data structure does not matter. 
	 */
	private void setSecondaryIndexes() {
		// Secondary index:
		// Mapping of candidates (index) -> selected data structure 
		// Otherwise: defaults to the parent.
		HashMap<Integer, EntityTree> secondaryIndexes = new HashMap<Integer, EntityTree>();
		
		for(Integer candidateIndex: queryDataMapping.keySet()) {
			LinkedList<Query> queryPath = queryDataMapping.get(candidateIndex.intValue());
			Candidate<T> candidate = candidates.get(candidateIndex.intValue());
			
			// FIND QUERY PATHS
			LinkedList<EntityTree> nodes = candidate.getCandidate().getQueryPaths(queryPath, true);
			
			// IF NO QUERY PATH HOWEVER QUERYPATH IS SIZE 2: TRY REVERSE TOO.
			if((nodes == null || nodes.isEmpty()) && queryPath.size() == 2) {
				LinkedList<Query> queryPathNew = new LinkedList<Query>();
				queryPathNew.add(queryPath.peekLast());
				queryPathNew.add(queryPath.peekFirst());
				
				nodes = candidate.getCandidate().getQueryPaths(queryPathNew, true);
				
				if(nodes != null && !nodes.isEmpty()) {
					EntityTree secondaryIndex = getBestSecondaryIndex(nodes);
					
					for(EntityTree child: secondaryIndex.getChildren()) {
						if(child.canQuery(queryPath.peekFirst())) {
							secondaryIndexes.put(candidateIndex, child);
							break;
						}
					}
				}
			}
			// Query path doesn't start at the top, select the best secondary index. (Min frequency, highest level).
			else if(nodes != null && !nodes.isEmpty() && !nodes.getFirst().equals(candidate.getCandidate())) {
				EntityTree secondaryIndex = getBestSecondaryIndex(nodes);
				
				// Check however if we haven't  queried any of the Nodes before the SecIndex. => Shifts up.
				secondaryIndex = shiftSecondaryIndexUp(candidateIndex, secondaryIndex);
				
				if(secondaryIndex.hasParent())
					secondaryIndexes.put(candidateIndex, secondaryIndex);
			}
		}
		
		this.secondaryIndexes = secondaryIndexes;
	}
	
	/**
	 * Suppose:
	 * [ Users | Bids ] -> [ Bids | Items | Users].
	 * Q0 = Users, Bids,    Q1 = Items; => We dont need a Secondary Index at C1 because we already know the bids.
	 * 
	 * @param candidateIndex
	 * @param secondaryIndex
	 * @return
	 */
	private EntityTree shiftSecondaryIndexUp(int candidateIndex, EntityTree secondaryIndex) {
		LinkedList<Query> latestQueriesExecuted = getQueriesBeforeCandidate(candidateIndex);
		
		int queriesPresent = 0;
		
		while(secondaryIndex.hasParent() && !latestQueriesExecuted.isEmpty()) {
			Query previousQuery = latestQueriesExecuted.removeLast();
			
			if(secondaryIndex.getParent().canQuery(previousQuery)) {
				secondaryIndex = secondaryIndex.getParent();
				queriesPresent++;
			}
		}
		
		this.previousQueriesPresent.put(new Integer(candidateIndex), new Integer(queriesPresent));
		
		
		return secondaryIndex;
	}
	
	/**
	 * First, identify the lowest / minimum node frequency.
	 * Then select the node at the highest level, which is the node occurring first.
	 * 
	 * @param nodes	EntityTrees sorted by breadth-first appearance and which allow to answer a partial query.
	 * @return
	 */
	private EntityTree getBestSecondaryIndex(LinkedList<EntityTree> nodes) {
		int min = Integer.MAX_VALUE;
		EntityTree bestNode = null;
		
		for(EntityTree node: nodes) {
			int nodeFrequency = node.getNodeFrequency();
			
			if(nodeFrequency < min) {
				min = nodeFrequency;
				bestNode = node;
			}
			
			if(min == 1)
				break;
		}
		
		return bestNode;
	}
	
	/**
	 * Returns true if there are any secondary indexes at some point.
	 * 
	 * @return
	 */
	public boolean hasSecondaryIndex() {
		return !this.secondaryIndexes.isEmpty();
	}

	/**
	 * Returns a mapping between:
	 * 
	 * - Position Index (candidate)
	 * - Required secondary indexes
	 * 
	 * Cost of a secondary index depends on cardinality of relationship:
	 * - 1-n is N, but 3-N is 3*N. Which is the node frequency.
	 */
	public HashMap<Integer, EntityTree> getSecondaryIndexes() {
		return new HashMap<Integer, EntityTree>(this.secondaryIndexes);
	}
	
	/**
	 * If there is a secondary index at the given candidate index,
	 * the function checks whether the query path is reversed,
	 * i.e. Query 2 comes before Query 1. 
	 * 
	 * Reversed paths can only occur with queries of length 2 at a candidate.
	 * @param index
	 * @return
	 */
	public boolean isPathReversedAtCandidateIndex(int index) {
		EntityTree secIndex = secondaryIndexes.get(new Integer(index));
		
		if(secIndex == null)
			return false;
		
		LinkedList<Query> mapping = queryDataMapping.get(new Integer(index));
		
		if(mapping.size() < 2 || mapping.size() > 2) 
			return false;
		
		if(secIndex.canQuery(mapping.peekFirst()) && secIndex.getParent().canQuery(mapping.peekLast()))
			return true;
		else
			return false;
	}
	
	/**
	 * Matches a candidate position with a query.
	 * 
	 * @param index
	 * @param query
	 */
	protected void addQueryMapping(int index, Query query) {
		if(!isValidIndex(index))
			return;
		
		if(!this.queryDataMapping.containsKey(new Integer(index))) 
			this.queryDataMapping.put(new Integer(index), new LinkedList<Query>());
		
		this.queryDataMapping.get(new Integer(index)).add(query);
	}
	
	/**
	 * Index has to be in range of the number of candidates.
	 * 
	 * @param index
	 * @return
	 */
	public boolean isValidIndex(int index) {
		if(index < 0 || index > (this.size()-1) )
			return false;
		
		return true;
	}
	
	/**
	 * Check if candidates can answer the query plan.
	 * TODO: Double check if correct.
	 * 
	 * Per candidate, we select certain attributes.
	 * 
	 * 1) The order of the attributes selected must appear in the sequence.
	 * 2) The order in which the attributes are selected must be possible:
	 * 	  - If first attribute is not parent => Secondary index has to be created.
	 *    - If a following attribute is on a higher basis: this can only be possible once, or if the relation to that point is 1-1.
	 *    - The attributes have to be connected, unless there is no guarantuee we intend the correct relationship.
	 * 
	 * Idea: attributes have to be connected.
	 * 
	 * TODO:
	 * Sequence: -1945716290, QueryPlan [candidates=[ items-875 [ bids-685 [ items-537 ] [ users186 ] ] ]], queryMapping={0=[Query [users], Query [items]]}, secondaryIndex={}]
	 * Sequence: -1945716290, QueryPlan [candidates=[ users186 [ bids-685 [ items-875 ] [ items-537 ] ] ]], queryMapping={0=[Query [users], Query [items]]}, secondaryIndex={}]
	 * 
	 * Not valid. Since bids -> items, is not the same as users -> items (intended).
	 * 
	 * 
	 * 
	 * @return
	 */
	public boolean isValidQueryPlan() {
		// Check if a query is mapped to at least each candidate.
		for(T cand: candidates) {
			int i=0;
			
			if(queryDataMapping.get(i) == null || queryDataMapping.get(i).isEmpty())
				return false;
		}
		
		for(Integer index: queryDataMapping.keySet()) {
			LinkedList<Query> queryPath = queryDataMapping.get(index.intValue());
			
			if(queryPath.size() != 2) {
				if(!candidates.get(index.intValue()).getCandidate().hasQueryPath(queryPath, true))
					return false;
			} else{
				queryPath = new LinkedList<Query>();
				queryPath.add(queryDataMapping.get(index.intValue()).peekLast());
				queryPath.add(queryDataMapping.get(index.intValue()).peekFirst());
				
				// if query path is size 2, switch allowed.
//				if(candidates.get(0).getCandidate().size() == 4 && this.candidates.size() == 1) {
//					System.out.println("");
//				}
//				
				
				//TODO: Check this.
				if(!candidates.get(index.intValue()).getCandidate().hasQueryPath(queryPath, true) &&
						!candidates.get(index.intValue()).getCandidate().hasQueryPath(queryDataMapping.get(index.intValue()), true)
						) 
					return false;
			}
				
		}
		
		
		
		return true;
	}
	
	public HashMap<Integer, LinkedList<Query>> getMapping() {
		return this.queryDataMapping;
	}
	
	
	/**
	 * Removes the data mapping at the given position,
	 * and places the selected information at the T destinationCandidate.
	 * 
	 * TODO: Test ?
	 * 
	 * @param index
	 * @return
	 */
	protected void removeCandidateAndTo(int removeIndex, int destinationIndex) {	
		if(!isValidIndex(removeIndex) || !isValidIndex(destinationIndex))
			return;
		
		int removeKey = new Integer(removeIndex);
		int destinationKey = new Integer(destinationIndex);
		
		if(!queryDataMapping.containsKey(removeKey) || !queryDataMapping.containsKey(destinationKey))
			return;
		
		// Insert queries in correct order.
		if(removeIndex > destinationIndex)
			queryDataMapping.get(destinationKey).addAll(queryDataMapping.get(removeKey));
		else {
			queryDataMapping.get(removeKey).addAll(queryDataMapping.get(destinationKey));
			queryDataMapping.put(destinationKey, queryDataMapping.get(removeKey));
		}
		
		// Remove candidate
		queryDataMapping.remove(removeKey);
		
		// Shift top candidates
		for(int k=removeIndex; k < (this.size()-1); k++) {
			queryDataMapping.put(new Integer(k), queryDataMapping.get(new Integer(k+1)));
		}

		this.queryDataMapping.remove(new Integer(this.size()-1));
		
		// Remove actual candidate
		this.candidates.remove(removeIndex);
		this.setSecondaryIndexes();
	}
	
	/**
	 * Replaces the given candidate's positions by the new candidate.
	 * 
	 * The query data mapping is unaffected, since it maps to positions.
	 * 
	 * @param candidate
	 * @param newCandidate
	 */
	protected void replaceCandidate(T candidate, T newCandidate) {
		for(int i=0; i < candidateOccurence(candidate); i++) {
			Integer indexKey = new Integer(this.candidates.indexOf(candidate));
			this.candidates.set(indexKey.intValue(), newCandidate);
		}
		
		this.setSecondaryIndexes();
	}
	
	/**
	 * Replaces all occurences of a given candidate by a new candidate once,
	 * and creates new query plan for each replacement.
	 * 
	 * @param candidate
	 * @param newCandidate
	 * @return
	 */
	protected Set<QueryPlan<T>> replaceCandidateOnceEachTime(T candidate, T newCandidate) {
		HashSet<QueryPlan<T>> newQPs = new HashSet<QueryPlan<T>>();
		
		for(int i=0; i < candidates.size(); i++) {
			if(candidates.get(i).equals(candidate)) {
				LinkedList<T> newCandidates = new LinkedList<T>(candidates);
				newCandidates.set(i, newCandidate);
				newQPs.add(this.clone(newCandidates));
			}
		}
		
		return newQPs;		
	}
	
	/**
	 * Replace a candidate by a given new candidate and generate all possible new query plans.
	 * 
	 * Multiple occurrences can take place.
	 * 
	 * @param candidate
	 * @param newCandidate
	 * 
	 * TODO: UNTESTED! (seems to work though)
	 */
	protected Set<QueryPlan<T>> replaceCandidateAllOptions(T candidate, T newCandidate) {
		Set<QueryPlan<T>> newQPs = new HashSet<QueryPlan<T>>();
		Stack<QueryPlan<T>> stackQPs = new Stack<QueryPlan<T>>();
		
		stackQPs.add(this.clone());
		
		// Per plan, create new plans with a replacement at each step possible.
		// Push new plans onto the stack if they still have old candidate occurrence left.
		while(!stackQPs.isEmpty()) {
			QueryPlan<T> workingQP = stackQPs.pop();
			
			Set<QueryPlan<T>> replacements = workingQP.replaceCandidateOnceEachTime(candidate, newCandidate);
			
			// If old candidates still left, push onto the stack.
			for(QueryPlan<T> replacement: replacements) {
				if(replacement.getCandidatesPlan().contains(candidate))
					stackQPs.add(replacement);
			}
			
			newQPs.addAll(replacements);
		}
		
		return newQPs;
	}
	
	/**
	 * New candidate was created. 
	 * 
	 * Create new query plans from this query plan based on the new available collection.
	 * 
	 * Cases (partial end, partial start, partial mid, or complete):
	 * [A|B] -> [C] -> [D|E|F] -> [E|F|G|H] -> [H|J]
	 * 
	 * 1) Candidate completely embedded in new candidate (e.g. [A|B|C]:
	 * => [A|B|C] -> [C] -> ..
	 * => [A|B] -> [A|B|C] -> ...
	 * after:  =>[A|B|C] -> [D|E|F] (Eliminated from newQP1)
	 * 2) Candidate partial and complete (e.g. [B|C|D])
	 * => [A|B] -> [B|C|D] -> [C] ...
	 * => [A|B] -> [B|C|D] -> [D|E|F] -> ...
	 * => [A|B] -> [C] -> [B|C|D] -> [D|E|F]
	 * 3) Partial in middle (e.g. [F,G]
	 * => [A|B] -> [C] -> [D|E|F] -> [F|G] -> [E|F|G|H]...
	 * => [A|B] -> [C] -> [D|E|F] -> [E|F|G|H] (=Nothing)
	 * 
	 * TODO: To do this first have to create relevant EntityTree functionality.
	 * 
	 * End: Compaction.
	 * 
	 * @param candidate
	 */
	public Set<QueryPlan<T>> notifyNewCandidate(T newCandidate) {
		Set<QueryPlan<T>> queryPlans = new HashSet<QueryPlan<T>>();
		
		ArrayList<T> traversedCandidates = new ArrayList<T>();
		
		for(T candidate: candidates) {
			// Multiple occurrences can be replaced in one-go.
			if(traversedCandidates.contains(candidate)) 
				continue;
			else
				traversedCandidates.add(candidate);
			
			// 1) Candidate completely embedded in new candidate
			if(candidate.isSubSetOf(newCandidate)) {
				Set<QueryPlan<T>> qpReplacements = this.replaceCandidateAllOptions(candidate, newCandidate);			
				
//				System.out.println("Embedded multiple completely in:" +candidate + ", new qp:" + qp);
				for(QueryPlan<T> qp: qpReplacements) {
					if(qp.isValidQueryPlan())
						queryPlans.add(qp);
				}
			}
			
			// TODO 2 and 3? Unless this is somehow facilitated via the generator.
			// Not completely necessary to implement I think.
		}
		
		boolean changed = true;
		
		while(changed) {
			changed = applyCompactions(queryPlans);
		}
		
		return queryPlans;
	}
	
	/**
	 * Compact QPs together:
	 * 
	 * For example:
	 * -> [User | Bids] -> Bids -> ...
	 * 
	 * Transfer indexes when needed(!).
	 * 
	 * Compaction applies:
	 * Users -> [Users|Bids] to [Users|Bids] => remove secondary index of users at candidate 0. 
	 * 
	 * 
	 * @param queryPlans
	 */
	private boolean applyCompactions(Set<QueryPlan<T>> queryPlans) {
		ArrayList<QueryPlan<T>> newQPs = new ArrayList<QueryPlan<T>>();
		ArrayList<QueryPlan<T>> removeQPs = new ArrayList<QueryPlan<T>>();
		
		for(QueryPlan<T> queryPlan: queryPlans) {
			LinkedList<T> planCandidates = queryPlan.getCandidatesPlan();
			
			for(int index=1; index < planCandidates.size(); index++) {
				// Check if [A|B] -> B exists, and if so create new with QP without B.
				// Not always possible. If B selects something different.
				// TODO:
				// IsValidQP? generated? or ... 
				// are elements in EntityTree in a good order? (IsValidOrder?)
				if(planCandidates.get(index).isSubTreeOf(planCandidates.get(index-1))) {
					QueryPlan<T> newQP = queryPlan.clone();
					
					newQP.removeCandidateAndTo(index, index-1);
					
					if(newQP.isValidQueryPlan()) {
						newQPs.add(newQP);
						removeQPs.add(queryPlan);
					}
				} 
				// Check if A -> [A|B] -> .. exists remove A
				// Check if B -> [A|B|C] -> Exists...
				else if(planCandidates.get(index-1).isSubTreeOf(planCandidates.get(index)) 
						&& planCandidates.get(index-1).isSubSetOf(planCandidates.get(index))) {					
					QueryPlan<T> newQP = queryPlan.clone();
					newQP.removeCandidateAndTo(index-1, index);
					
					if(newQP.isValidQueryPlan()) {
						newQPs.add(newQP);
						removeQPs.add(queryPlan);
					}
				}
				
				//TODO may exist like this [A|B|C] -> B -> C ->..
			}
		}
		
		queryPlans.removeAll(removeQPs);
		queryPlans.addAll(newQPs);
		
		return !removeQPs.isEmpty() || !newQPs.isEmpty();
	}

	/**
	 * Returns the number of times a candidate occurs in the plan.
	 * 
	 * @param candidate
	 * @return
	 */
	public int candidateOccurence(T candidate) {
		int count = 0;
		
		for(T cand: candidates) {
			if(cand.equals(candidate))
				count++;
		}
		
		return count;
	}

	public void subscribeToCandidates() {
		for(T candidate: candidates) {
			candidate.addSubscribedQP(this);
		}
	}
	
	public void unsubscribeToCandidates() {
		for(T candidate : candidates) {
			candidate.removeSubscribedQP(this);
		}
	}
	
	/**
	 * 
	 * Checks if other query plan is more efficient than this current plan.
	 * 
	 * @param qp
	 * @return
	 */
	public boolean supersededBy(QueryPlan<T> qp) {	
		if(!qp.getSequence().equals(qp.getSequence()))
			return false;
		
		for(int index=0; index<candidates.size(); index++) {
			LinkedList<T> newPlan = new LinkedList<T>(candidates);
			newPlan.remove(index);
			
			QueryPlan<T> newQP = new QueryPlan<T>(sequence, newPlan);
			
			// if new QP is intended for the same sequence, and has the same set of candidates.
			// TODO: we should also perhaps check that queryDataMapping is the same, except for index, index-1, and index+1?
			if(newQP.getCandidatesPlan().equals(qp.getCandidatesPlan())) 
				return true;
		}
		
		return false;
	}
	
	/**
	 * Create new candidate collections to optimize this query plan.
	 */
	public Set<T> optimize() {
		HashSet<T> newCandidates = new HashSet<T>();
		
		for(int i=0; i < candidates.size()-1; i++) {
//			System.out.println(candidates.get(i));
//			System.out.println(candidates.get(i+1));
//			System.out.println(candidates.get(i).canMerge(candidates.get(i+1)));
//			
			if(!candidates.get(i).isSubSetOf(candidates.get(i+1)) && candidates.get(i).canMerge(candidates.get(i+1))) {
				T newCandidate = candidates.get(i).merge(candidates.get(i+1));
//				System.out.println(newCandidate);
//				System.out.println(newCandidate.getCandidate().isValidCyclic());
				
				// - Candidate must be within max depth, and max width configured
				// - Candidate must not exceed maximum embedded documents.
				// - Candidate's cyclic elements must be valid. (i.e. in demand by sequences)
				// - And new candidate can actually replace the previous 2. (FCC).
				if(newCandidate.getCandidate().getMaxDepth() <= Config.MAX_DOCUMENT_DEPTH && newCandidate.getCandidate().getMaxWidth() <= Config.MAX_DOCUMENT_WIDTH
						&& newCandidate.getCandidate().getNodesTotalFrequency() <= Config.MAX_SINGLE_DOCUMENT_EMBEDDED_AMOUNT 
						&& newCandidate.getCandidate().isValidCyclic()  
						// && !newCandidate.getCandidate().hasReoccurringElements() 
						&& candidates.get(i).isSubSetOf(newCandidate) && candidates.get(i+1).isSubSetOf(newCandidate)) {
					
	
					newCandidates.add(newCandidate);
				}
			}
		}
		
		return newCandidates;
	}
	
	public int size() {
		return this.candidates.size();
	}
	
	/**
	 * Calculates the cost of all queries executed in the plan.
	 * 
	 */
	@Override
	public int getCost() {
		CostCalculatorPlan<T> calculator = new CostCalculatorPlan<T>(this);
		
		return calculator.getCost();
	}

	@Override
	public String toString() {
		String string = "Rank: " +  getRank() + "Valid: " + isValidQueryPlan() + ", Cost:" + getCost() +  ", Sequence: " +  sequence.hashCode() + ", QueryPlan [candidates=";
		
		for(T cand: candidates) {
			string += cand + " -> ";
		}
		
		
		string = string.subSequence(0, string.length()-4).toString() + "], queryMapping=" + queryDataMapping + ", secondaryIndex=" + secondaryIndexes +  "]";
		
		return string;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((candidates == null) ? 0 : candidates.hashCode());
		result = prime * result + ((queryDataMapping == null) ? 0 : queryDataMapping.hashCode());
		result = prime * result + ((sequence == null) ? 0 : sequence.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryPlan other = (QueryPlan) obj;
		if (candidates == null) {
			if (other.candidates != null)
				return false;
		} else if (!candidates.equals(other.candidates))
			return false;
		if (queryDataMapping == null) {
			if (other.queryDataMapping != null)
				return false;
		} else if (!queryDataMapping.equals(other.queryDataMapping))
			return false;
		if (sequence == null) {
			if (other.sequence != null)
				return false;
		} else if (!sequence.equals(other.sequence))
			return false;
		return true;
	}

	@Override
	public int compareTo(QueryPlan<T> o) {
		if(o.equals(this))
			return 0;
		
		if(o.size() > this.size())
			return -1;
		
		if(o.size() < this.size())
			return 1;
		
		return -1;
	}	
	
	@Override
	protected QueryPlan<T> clone() {
		return clone(getCandidatesPlan());
	}
	
	/**
	 * Clone this query plan with a new set of candidates (optionally), typically used for replace.
	 * 
	 * @param candidates
	 * @return
	 */
	protected QueryPlan<T> clone(LinkedList<T> candidates) {
		LinkedList<T> newCandidates = new LinkedList<T>(candidates);
		
		HashMap<Integer, LinkedList<Query>> newMapping = new HashMap<Integer, LinkedList<Query>>();
		
		for(Integer key: queryDataMapping.keySet()) {
			LinkedList<Query> mappedQueries = new LinkedList<Query>();
			
			if(queryDataMapping.get(key) != null)
				mappedQueries.addAll(queryDataMapping.get(key));
			
			newMapping.put(new Integer(key.intValue()), mappedQueries);
		}
		
		QueryPlan<T> newQP = new QueryPlan<>(sequence, newCandidates, newMapping);
		
		return newQP;
	}

	@Override
	public Integer getRank() {
		return this.rank;
	}
	
	@Override
	public void setRank(Integer rank) {
		this.rank = rank;
	}

	@Override
	public void setFitness(Integer currentRank, Integer totalSeqRank) {
		this.fitness = calculateLinearBucketFitnessScore(currentRank, totalSeqRank);
	}
	
	@Override
	public double getFitness() {
		return this.fitness;
	}
	
	/**
	 * % http://www.pohlheim.com/Papers/mpga_gal95/gal2_3.html
	 * @param currentRank
	 * @param totalSeqRank
	 */
	private double calculateLinearFitnessScore(Integer currentRank, Integer totalSeqRank) {
		int SP = 2;
		
		double fitness = 2-SP+2*(SP-1)*(currentRank)/(totalSeqRank);
		
		return fitness;
	}
	
	/**
	 * Rank is distributed according to the bucket the QP is in.
	 * @param currentRank
	 * @param totalSeqRank
	 * @return
	 */
	private double calculateLinearBucketFitnessScore(Integer currentRank, Integer totalSeqRank) {
		double fitness = calculateLinearFitnessScore(currentRank, totalSeqRank);
		
		return fitness / ((getQueries().size() * getQueries().size()));
	}
	
	/**
	 * http://www.geatbx.com/docu/algindex-02.html#P249_16387
	 * 
	 * Faulhaber's formula.
	 * 
	 * TODO: Doesn't work yet, overflow.
	 * 
	 * @param currentRank
	 * @param totalSeqRank
	 * @return
	 */
	private double calculateNonLinearFitnessScore(Integer currentRank, Integer totalSeqRank) {
		double X = Math.exp(1);
		int SP = 2;
		
//		
//		System.out.println("non linear function");
//		System.out.println("tot rank: " + totalSeqRank);
//		System.out.println("current rank: " + currentRank);
//		System.out.println("X:" + X);
//		
		double exponent = (1.0 + 8.0 * ( (double) currentRank / (double)  totalSeqRank));
//		System.out.println(exponent);
		
		double fitness = Math.pow(X, exponent );
//		System.out.println(fitness);
		// fitness = fitness / (( 1 - Math.pow(X, totalSeqRank)) / (1 - totalSeqRank));
		//System.out.println(fitness);
		
		return fitness;
	}
	
}
