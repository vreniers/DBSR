package dbsr.candidate.generator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import dbsr.candidate.CandidateSequence;
import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;

/**
 * Start with the denormalized structure. 
 * E.g. users -> bids -> items -> users.
 * 
 * C1 = users, C2= bids, C3= items, C4= users.
 * 
 * Candidate keeps the associated query => determines fields used.
 * Establish the root Query Plan: C1 -> C2 -> C3 -> C4.
 * 
 * ------ Optimization Round 1 ---------
 * 
 * Query plan(!) suggest a new candidate to optimize itself:
 * Root: [C1,C2] and generates a new QP:
 * 
 * Notify all other existing Query Plans to the presence of this new Candidate !
 * => for(QP qp): addNewQPs(qp.notifyNewCandidate([C1,C2])) 
 * => Create new potential query plans.
 * 
 * Stack:
 * QP1: C1 -> C2 -> C3 -> C4
 * QP2: [C1,C2] -> C3 -> C4.
 * QP3: C1 -> [C1,C2] -> C3 -> C4? => Yes?
 * 
 * Now continue at top of QPs:
 * => Root yields [C2, C3].
 * => Notify all QPs: yields new 
 * 
 * QP4: [C1] -> [C2, C3] -> [C4], 
 * QP5: [C1,C2] -> [C2, C3] -> [C4].
 * QP6: C1 -> [C1,C2] -> [C2,C3] -> C4.
 * 
 * Continue with optimization of root QP:
 * => Combine [C3,C4]
 * => Notify all QPs; Yields first new 
 * 
 * QP7: [C1] -> [C2] -> [C3,C4]
 * QP8: [C1,C2] -> [C3,C4] 
 * QP9: C1 -> [C1,C2] -> [C3] -> [C3, C4]
 * QP : C1 -> [C1,C2] -> [C3,C4]
 * QP:  C1 -> [C2, C3] -> [C3,C4] => EXISTS.
 * QP:  C1 -> [C1,C2] -> [C2,C3] -> [C3,C4].
 * 
 * Root query: done -> continue now with new QP.
 * Until (no QP left to optimize! = Stack.pop()).
 * 
 * ------ Optimization Round 2 --------
 * 
 * Start:
 * QP2: [C1,C2] -> C3 -> C4. (Priority? [C3,C4] lower-level first?)
 * 
 * Candidate: [C3,C4] -> Exists => Scratch
 * Candidate: [C1,C2,C3]
 * 
 * QP: [C1,C2,C3] -> C4.
 * 
 * Notify subscribers of C1, C2, C3 -> which notify [C1,C2], [C2,C3] subscribers, ...
 * 
 * QP: C1 -> [C1,C2,C3] -> C3 -> C4
 * QP: [C1,C2] -> [C1,C2,C3] -> C4
 * from QP: [C1] -> [C2] -> [C3,C4] 
 * 	=> [C1,C2,C3] -> [C3,C4] and [C1] -> [C1,C2,C3] -> [C3,C4] and [C1] -> [C2] -> [C1,C2,C3] -> [C4]
 * ...
 * Case:
 * [C1,C2,C3] -> [C2,C3,C4] ?
 * 
 * @author root
 *
 */
public class SequenceCandidateGenerator {
	
	private final Sequence sequence;
	
	private Set<CandidateSequence> candidates = new HashSet<CandidateSequence>();
	
	/**
	 * Set of QueryPlans sorted by their length.
	 */
	private TreeSet<QueryPlan<CandidateSequence>> existingQueryPlans = new TreeSet<QueryPlan<CandidateSequence>>();
	
	private LinkedList<QueryPlan<CandidateSequence>> queryPlans = new LinkedList<QueryPlan<CandidateSequence>>();
	
	public SequenceCandidateGenerator(Sequence seq) {
		this.sequence = seq;
		
		createRootCandidates();
	}
	
	public Set<CandidateSequence> getCandidates() {
		return candidates;
	}

	public Set<QueryPlan<CandidateSequence>> getExistingQueryPlans() {
		return existingQueryPlans;
	}
	
	/**
	 * Creates the initial set of candidates.
	 */
	private void createRootCandidates() {
		LinkedList<CandidateSequence> rootCandidates = new LinkedList<CandidateSequence>();
		QueryPlan<CandidateSequence> rootPlan = new QueryPlan<CandidateSequence>(sequence, rootCandidates);
		
		for(Query query: sequence.getQueryPath()) {
			EntityTree tree = new EntityTree(query.getEntity());
			tree.addQuery(query);
			
			CandidateSequence candidate = new CandidateSequence(tree, sequence);
			candidate.addSubscribedQP(rootPlan);
			rootCandidates.add(candidate);
		}
		
		addCandidates(rootCandidates);
		addQueryPlan(rootPlan);
	}
	
	/**
	 * Start popping the root query plan and inserting new query plans until
	 * the stack is no longer altered.
	 */
	public void startGeneration() {
		while(!queryPlans.isEmpty()) {
			System.out.println("--- Iteration ---");
			QueryPlan<CandidateSequence> root = queryPlans.removeFirst();
			
			Set<CandidateSequence> newCandidates = root.optimize();
			
			for(CandidateSequence candidate: newCandidates) {
				TreeSet<QueryPlan<CandidateSequence>> iterationQueryPlans = new TreeSet<QueryPlan<CandidateSequence>>();
				
				if(!candidates.contains(candidate)) {
					System.out.println("Candidate: " + candidate);
					candidates.add(candidate);
					Set<QueryPlan<CandidateSequence>> qps = candidate.notifySubscribers(candidate);
					
					for(QueryPlan<CandidateSequence> qp: qps) {
						iterationQueryPlans.add(qp);
					}
					
					compactQueryPlans(iterationQueryPlans);
					
					for(QueryPlan<CandidateSequence> qp: iterationQueryPlans)
						addQueryPlan(qp);
				}
			}
			
//			System.out.println(queryPlans);
			
			// Compact QueryPlans based on general info?
			// E.g.
			// QueryPlan [candidates=[ users ] -> [ bids ] -> [ bids [ items [ users ] ] ], indexes=[]]
			// This exists: [users] -> [ bids [ items [ users ] ] ]
			// So scratch it?
			// TODO the above 
			// TODO secondary indexing
			compactQueryPlans(existingQueryPlans);
			
		}
		
		System.out.println("---End generation---");
	}
	
	/**
	 * Sorts existing query plans by length, and checks if e.g. query plans of length 6 supersede a query plan of length 7.
	 * 
	 * Example:
	 * [candidates=[ users [ bids [ items [ users ] ] ] ] -> [ users ]  (superseded)
	 * [candidates=[ users [ bids [ items [ users ] ] ] ] ]
	 */
	private void compactQueryPlans(Set<QueryPlan<CandidateSequence>> qps) {
		HashSet<QueryPlan<CandidateSequence>> removeQPs = new HashSet<QueryPlan<CandidateSequence>>();

		Iterator<QueryPlan<CandidateSequence>> iteratorNewQPs = qps.iterator();
		
		while(iteratorNewQPs.hasNext()){
			QueryPlan<CandidateSequence> newQP = iteratorNewQPs.next();
			Iterator<QueryPlan<CandidateSequence>> iterator = existingQueryPlans.iterator();
			
			// Check if this newQP is superseded by an existing QP.
			while(iterator.hasNext()) {
				QueryPlan<CandidateSequence> qp = iterator.next();
				
				if(newQP.size() == (qp.size()+1)) {
					if(newQP.supersededBy(qp))
						removeQPs.add(newQP);
				}
				else if(newQP.size() > (qp.size() +1)) {
					break;
				}
			}
		}
		
		qps.removeAll(removeQPs);
	}
	
	/**
	 * Adds candidates.
	 */
	private void addCandidates(List<CandidateSequence> candidates) {
		this.candidates.addAll(candidates);
	}
	
	/**
	 * Adds a query plan.
	 */
	private void addQueryPlan(QueryPlan<CandidateSequence> qp) {
		if(existingQueryPlans.contains(qp))
			return;
		
		qp.subscribeToCandidates();
		
		System.out.println("add: " + qp);
		this.existingQueryPlans.add(qp);
		
		if(qp.size() > 1)
			this.queryPlans.add(qp);
	}
	
}
