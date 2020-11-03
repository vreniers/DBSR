package dbsr.candidate.generator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;


import dbsr.candidate.Candidate;
import dbsr.candidate.CandidateSequences;
import dbsr.candidate.generator.optimizations.QueryPlanNotifier;
import dbsr.config.Config;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;

/**
 * Processes multiple sequences at a time.
 * 
 * Starts with a root candidate for every sequence.
 * Then merges similar root candidates, until no merge is left possible.
 * 
 * E.g. 
 * user_Q1 + user_Q2
 * user_Q3 + user_Q1 
 * and again addition of these.
 * 
 * TODO:
 * When is user_Q1 = User_Q2?
 * 
 * User_Q3 can = User_Q1 + User_Q3 
 * or even User_Q3 = User_Q1 + User_Q2 
 * 
 * if User_Q3 is a superset already. 
 * 
 * Followed by the typical candidate generation process.
 * 
 * @author root
 *
 */
public class MultiSequenceCandidateGenerator {
	
	private final Set<Sequence> sequences;
	
	private Set<CandidateSequences> candidates = new HashSet<CandidateSequences>();
	
	/**
	 * Set of QueryPlans sorted by their length.
	 */
	private HashMap<Sequence, QueryPlansBucket> existingQueryPlans = new HashMap<Sequence, QueryPlansBucket>();
	
	/**
	 * All generated query plans, prevents re-generation of a removed QP.
	 */
	private HashSet<QueryPlan<CandidateSequences>> generatedQueryPlans = new HashSet<QueryPlan<CandidateSequences>>();
	
	/**
	 * Initial list of candidates per entity (table e.g. users).
	 * 
	 * Each EntityTree is distinct from another one in the list of values.
	 */
	private final HashMap<Entity, Set<EntityTree>> initialCandidatesPerEntity;

	/**
	 * All remaining query plans to evaluate for optimization.
	 */
	private LinkedList<QueryPlan<CandidateSequences>> queryPlansStack = new LinkedList<QueryPlan<CandidateSequences>>();
	
	/**
	 * Collection of queries affecting a single table (entity).
	 */
	private final HashMap<Entity, Set<Query>> queriesPerEntity;
	
	/**
	 * List of entities (tables).
	 */
	private final Set<Entity> entities;
	
	/**
	 * Initializes the generator with a set of sequences.
	 * 
	 * TODO: Important that if there are identical queries used across multiple sequences, they should be the same object.
	 * 
	 * @param sequences
	 */
	public MultiSequenceCandidateGenerator(Set<Sequence> sequences) {
		this.sequences = sequences;
		this.entities = getEntities(sequences);
		this.queriesPerEntity = getQueriesPerEntity(sequences);
		
		// Create Entity - Candidates HashMap function
		// for Entity; createEntityCandidates....
		this.initialCandidatesPerEntity = new HashMap<Entity, Set<EntityTree>>();
		
		for(Entity entity: entities) {
			Set<EntityTree> treeCandidates = this.getInitialCandidatesPerEntity(entity);
			System.out.println(entity);
			System.out.println(treeCandidates);
			this.initialCandidatesPerEntity.put(entity, treeCandidates);
		}
		
		System.out.println(initialCandidatesPerEntity);
		
		
		for(Sequence seq: sequences) {
			createRootQueryplans(seq);
		}
	}
	
	/**
	 * Groups all related queries on the same table (entity).
	 * 
	 * @param sequences
	 */
	private HashMap<Entity, Set<Query>> getQueriesPerEntity(Set<Sequence> sequences) {
		HashMap<Entity, Set<Query>> queriesPerEntity = new HashMap<Entity,Set<Query>>();
		
		for(Sequence seq: sequences) {
			LinkedList<Query> queries = seq.getQueryPath();
			
			for(Query qry: queries) {
				if(!queriesPerEntity.containsKey(qry.getEntity())) {
					Set<Query> queryList = new HashSet<Query>();
					queryList.add(qry);
					
					queriesPerEntity.put(qry.getEntity(), queryList);
				} else {
					queriesPerEntity.get(qry.getEntity()).add(qry);
				}
			}
		}
		
		return queriesPerEntity;
	}
	
	/**
	 * Creates possible candidate tables based on the list of queries affecting a table (entity).
	 * 
	 * TODO: Optimization? E.g. reduce number of max candidates to e.g. 10; calculate distance between and merge candidates.
	 * 
	 * @param entity
	 */
	private Set<EntityTree> getInitialCandidatesPerEntity(Entity entity) {
		Set<Query> queries = this.queriesPerEntity.get(entity);		
		HashMap<Set<Field>, EntityTree> candidates = new HashMap<Set<Field>, EntityTree>();
		
		/**
		 * HashMap<Set<Field>, Candidate> ?
		 */
		for(Query qry: queries) {
			ArrayList<Query> qryList = new ArrayList<Query>();
			qryList.add(qry);
			
			EntityTree newTree = new EntityTree(qry.getEntity(), qryList);
			Set<Field> affectedFields = newTree.getAffectedFields();
			
			System.out.println(qry);
			System.out.println(newTree.getAffectedFields());
			
			if(!candidates.containsKey(affectedFields))
				candidates.put(affectedFields, newTree);
			
			for(EntityTree tree: candidates.values()) {
				newTree = tree.clone();
				newTree.addQuery(qry);
				
				if(!tree.getAffectedFields().equals(newTree.getAffectedFields()))
					if(!candidates.containsKey(newTree.getAffectedFields()))
						candidates.put(newTree.getAffectedFields(), newTree);
			}
		}
		
		Set<EntityTree> treeCandidates = new HashSet<EntityTree>();
		treeCandidates.addAll(candidates.values());
		return treeCandidates;
	}
	
	/**
	 * Creates all possible query pathings over EntityTrees for a given query path.
	 * 
	 * 
	 * @param queryPath
	 * @return
	 */
	private Set<LinkedList<EntityTree>> getPossibleQueryPlans(LinkedList<Query> queryPath) {
		// First step, create basic stack of Q0 candidates.
		Iterator<Query> queryIterator = queryPath.iterator();
		Query qry = queryIterator.next();
		
	
		Set<EntityTree> candidates = this.initialCandidatesPerEntity.get(qry.getEntity());
		
//		Candidate EntityTrees
//		for(EntityTree et: candidates)
//			System.out.println(et.getQueries());
		
		HashSet<LinkedList<EntityTree>> queryPathings = new HashSet<LinkedList<EntityTree>>();
		
		for(EntityTree candidate: candidates) {
			if(candidate.canQuery(qry)) {
				LinkedList<EntityTree> candidatePathing = new LinkedList<EntityTree>();
				candidatePathing.add(candidate);
				queryPathings.add(candidatePathing);
			}
		}
				
		// Next steps, check each candidate, add new candidate option if possible. (can grow N*N).
		while(queryIterator.hasNext()) {
			qry = queryIterator.next();
			HashSet<LinkedList<EntityTree>> newQueryPathings = new HashSet<LinkedList<EntityTree>>();
			
			candidates = this.initialCandidatesPerEntity.get(qry.getEntity());
			
			for(EntityTree candidate: candidates) {
				if(candidate.canQuery(qry)) {
					// Add this candidate step to all PREVIOUS steps. (= LARGE)
					for(LinkedList<EntityTree> path: queryPathings) {
						LinkedList<EntityTree> newPath = new LinkedList<EntityTree>(path);
						newPath.add(candidate);
						newQueryPathings.add(newPath);
					}
				}
			}
			
			queryPathings = newQueryPathings;
		}
		
		return queryPathings;
	}
	
	/**
	 * Creates the initial set of candidates.
	 * 
	 * @param Sequence
	 * 		  Create root candidates for a given sequence.
	 */
	private void createRootQueryplans(Sequence sequence) {
		
		System.out.println(sequence);
		
		Set<LinkedList<EntityTree>> queryPathings = getPossibleQueryPlans(sequence.getQueryPath());
		Set<QueryPlan<CandidateSequences>> rootPlans = new HashSet<QueryPlan<CandidateSequences>>();
		
		// Per path: Create query plan and candidates based on a path of EntityTrees.
		for(LinkedList<EntityTree> queryPath : queryPathings) {
			
			LinkedList<CandidateSequences> rootCandidates = new LinkedList<CandidateSequences>();
			QueryPlan<CandidateSequences> rootPlan = new QueryPlan<CandidateSequences>(sequence, rootCandidates);		
			
			for(EntityTree candidateTree: queryPath) {
				CandidateSequences candidate = new CandidateSequences(candidateTree);
				
				if(candidates.contains(candidate)) {
					for(CandidateSequences cand: candidates) {
						if(cand.equals(candidate)) {
							candidate = cand;
						}
					}
				}
				
				// Create query data mapping
				LinkedList<Query> queryMap = new LinkedList<Query>();
				queryMap.add(sequence.getQueryPath().get(rootPlan.getMapping().size()));
				rootPlan.getMapping().put(rootPlan.getMapping().size(), queryMap);
				
				rootPlans.add(rootPlan);
				rootCandidates.add(candidate);
			}
			
			System.out.println(rootPlan);
			
			addCandidates(rootCandidates);
			addQueryPlans(rootPlans);
		}
	}
	
	public void startGeneration() {
		long startTime = System.currentTimeMillis();
		startGeneration(Config.MAX_ITERATIONS);
		
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println("Execution time" + totalTime);
	}
	
	/**
	 * Start popping the root query plan and inserting new query plans until
	 * the stack is no longer altered.
	 */
	public void startGeneration(int cycles) {
		while(!queryPlansStack.isEmpty() && cycles > 0) {
			cycles--;
			
//			printQueryPlansPerSequence();
			
			System.out.println("--- Iteration ---" + cycles);
			System.out.println(queryPlansStack.size());
			QueryPlan<CandidateSequences> root = queryPlansStack.removeFirst();
			
//			System.out.println("Optimizing: " + root);
			Set<CandidateSequences> newCandidates = root.optimize();
			
//			System.out.println("New Candidates: " + newCandidates);
			
			for(CandidateSequences candidate: newCandidates) {				
				if(candidates.contains(candidate)) {
					continue;
				}
				
				TreeSet<QueryPlan<CandidateSequences>> iterationQueryPlans = new TreeSet<QueryPlan<CandidateSequences>>();
				Set<Sequence> sequencesAffected = new HashSet<Sequence>();
				
				// Add candidate, notify subscribed QPs of new data structure.
				if(!candidates.contains(candidate)) {
					candidates.add(candidate);
					Set<QueryPlan<CandidateSequences>> newQPs;
					
					if(Config.MULTI_THREADING_NOTIFIER) {
						QueryPlanNotifier<CandidateSequences> notifier = new QueryPlanNotifier<CandidateSequences>(candidate, generatedQueryPlans);
						newQPs = notifier.getNewQueryPlans();
					} else {
						newQPs = candidate.notifySubscribers(candidate);
						newQPs.removeAll(generatedQueryPlans);
					}
										
					generatedQueryPlans.addAll(newQPs);					
					
					for(QueryPlan<CandidateSequences> qp: newQPs) {
						addQueryPlan(qp);
					}
				}
			}
			
			pruneOrCompact(cycles);
		}
		
		System.out.println("---End generation---");
	}
	
	/**
	 * Removes the query plan, unsubcribes the QP to all its candidates used.
	 * 
	 * Removes the query plan from the stack, and from the existing plans.
	 * 
	 * @param removeQPs
	 */
	private void removeQueryPlans(Sequence seq, Collection<QueryPlan<CandidateSequences>> removeQPs) {
		for(QueryPlan<CandidateSequences> qp: removeQPs) {
//			System.out.println("Remove: " + qp);
			qp.unsubscribeToCandidates();
		}
		
//		System.out.println(removeQPs.size());
		existingQueryPlans.get(seq).removeQueryPlans(removeQPs);
		queryPlansStack.removeAll(removeQPs);	
		
	}
	
	/**
	 * Removes the query plan, unsubcribes the QP to all its candidates used.
	 * 
	 * Removes the query plan from the stack, and from the existing plans.
	 * 
	 * @param removeQPs
	 */
	private void removeQueryPlan(QueryPlan<CandidateSequences> removeQP) {
		removeQP.unsubscribeToCandidates();
		
		existingQueryPlans.get(removeQP.getSequence()).removeQueryPlan(removeQP);
		queryPlansStack.remove(removeQP);			
	}

	/**
	 * Adds candidates.
	 */
	private void addCandidates(List<CandidateSequences> candidates) {
		this.candidates.addAll(candidates);
	}
	
	private void addQueryPlans(Collection<QueryPlan<CandidateSequences>> qps) {
		for(QueryPlan<CandidateSequences> qp: qps)
			addQueryPlan(qp);
	}
	
	/**
	 * Adds a query plan.
	 */
	private void addQueryPlan(QueryPlan<CandidateSequences> qp) {	
		if(!existingQueryPlans.containsKey(qp.getSequence()))
			existingQueryPlans.put(qp.getSequence(), new QueryPlansBucket());
		
		if(existingQueryPlans.get(qp.getSequence()).contains(qp))
			return;
		
		if(queryPlansStack.contains(qp))
			return;
		
		qp.subscribeToCandidates();
		
//		System.out.println("add: " + qp);
		this.existingQueryPlans.get(qp.getSequence()).addQueryPlan(qp);
		
		if(qp.size() > 1)
			this.queryPlansStack.add(qp);
	}
	
	public Set<CandidateSequences> getCandidates() {
		return candidates;
	}
	
	public Set<Sequence> getSequences() {
		return sequences;
	}
	
	public Set<QueryPlan<CandidateSequences>> getExistingQueryPlan(Sequence sequence) {
		return this.existingQueryPlans.get(sequence).getQueryPlans();
	}
	
	public HashMap<Sequence, QueryPlansBucket> getQueryPlansPerSequence() {
		return this.existingQueryPlans;
	}
	
	public void printQueryPlansPerSequence() {
		for(Sequence seq: sequences) {
			System.out.println(seq.hashCode());
			for(QueryPlan<CandidateSequences> qp: getExistingQueryPlan(seq)) {
				
				if(qp.getSequence().equals(seq))
					System.out.println(qp);
			}
		}
	}
	
	public void printCandidates() {
		for(CandidateSequences cand: candidates) {
			System.out.println(cand);
		}
	}
	
	/**
	 * Can be called at the end of a generation process.
	 * 
	 * Filters best query plans per sequence, drops unused data structures.
	 * Requires ranking.
	 * 
	 * TODO: Implement
	 */
	public void getRecommendation() {
		
	}
	
	/**
	 * Returns the set of working entities (tables).
	 * 
	 * @param sequences
	 * @return
	 */
	private Set<Entity> getEntities(Set<Sequence> sequences) {
		Set<Entity> entities = new HashSet<Entity>();
		
		for(Sequence seq: sequences) {
			for(Query qry: seq.getQueryPath())
				entities.add(qry.getEntity());
		}
		
		return entities;
	}
	
	/**
	 * Used in the generation at certain intervals to merge, compact, or prune query plans or data structures.
	 * 
	 * @param cycles
	 */
	private void pruneOrCompact(int cycles) {
		
		if(cycles % 800 == 0 || queryPlansStack.isEmpty()) {
			for(Sequence seq: existingQueryPlans.keySet()) {
//				System.out.println("compacting");
				
				int size = existingQueryPlans.get(seq).size();
				compactQueryPlans(seq, existingQueryPlans.get(seq).getQueryPlans());
				
				Pruner pruner = new Pruner(this);
				pruner.beforePruning();
				
				if(size != existingQueryPlans.get(seq).size()) {
					System.out.println("Compacted: " + (size - existingQueryPlans.get(seq).size()));
				}
			}
		}
		
		// Prune threshold documents.
		if(queryPlansStack.isEmpty() || cycles == 0) {
			Pruner pruner = new Pruner(this);
			pruner.beforePruning();

			System.out.println("Pruned data structures:" + candidates.size());
			pruner.pruneDataStructures();
			System.out.println("Size data structures: " + candidates.size());
			
			pruner = new Pruner(this);
			pruner.beforePruning();
		}
		
		if(queryPlansStack.size() > Config.PRUNE_AT_QUERY_STACK_SIZE || queryPlansStack.isEmpty()) {
			Pruner pruner = new Pruner(this);
			pruner.beforePruning();
			
			pruner.pruneQueryPlans();
			pruner.beforePruning();
			
		}
	}
	
	/**
	 * Sorts existing query plans by length, and checks if e.g. query plans of length 6 supersede a query plan of length 7.
	 * 
	 * Example:
	 * [candidates=[ users [ bids [ items [ users ] ] ] ] -> [ users ]  (superseded)
	 * [candidates=[ users [ bids [ items [ users ] ] ] ] ]
	 */
	private void compactQueryPlans(Sequence seq, Set<QueryPlan<CandidateSequences>> qps) {
		List<QueryPlan<CandidateSequences>> removeQPs = new ArrayList<QueryPlan<CandidateSequences>>();

		Iterator<QueryPlan<CandidateSequences>> iteratorNewQPs = qps.iterator();
		
		while(iteratorNewQPs.hasNext()){
			QueryPlan<CandidateSequences> newQP = iteratorNewQPs.next();
			
			if(!newQP.getSequence().equals(seq))
				continue;
			
			Set<QueryPlan<CandidateSequences>> setQPs = existingQueryPlans.get(seq).getQueryPlansOfSize(newQP.size() - 1);
			
			if(setQPs == null || setQPs.isEmpty())
				continue;
			
			Iterator<QueryPlan<CandidateSequences>> iterator = setQPs.iterator();
			
			// Check if this newQP is superseded by an existing QP of size - 1.
			while(iterator.hasNext()) {
				QueryPlan<CandidateSequences> qp = iterator.next();
				if(newQP.supersededBy(qp)) {
//					System.out.println("--- superseded qp:----");
//					System.out.println(newQP);
//					System.out.println("by: " + qp);
					removeQPs.add(newQP);
				}
			}
		}
		
		removeQueryPlans(seq, removeQPs);
	}
	
	/**
	 * Functionality to determine a query plan's rank per sequence, and overall for all sequences.
	 * 
	 */
	private class Pruner{
		
		private final MultiSequenceCandidateGenerator gen;
		
		private HashMap<CandidateSequences, LinkedHashMap<Sequence, Double>> fitnessPerCandidate;
		
		public Pruner(MultiSequenceCandidateGenerator gen) {
			this.gen = gen;
		}
				
		public void beforePruning() {
			HashMap<Sequence, QueryPlansBucket> queryPlansPerSequence = gen.getQueryPlansPerSequence();
			
			// Calculate all the ranks of the QueryPlans.
			for(Sequence seq: queryPlansPerSequence.keySet()) {
				queryPlansPerSequence.get(seq).precalculateQueryPlanRanks();
			}
	
			this.fitnessPerCandidate = getFitnessForDataStructures();
		}
		
		/**
		 * Only execute during pruning! Costly function.		 * 
		 */
		public HashMap<CandidateSequences,Integer> getRanksForDataStructures() {	
			HashMap<CandidateSequences, Integer> ranksPerCandidate = new HashMap<CandidateSequences, Integer>();
			
			// keep sorted list of keys of QP costs. (sometimes weights overlap multiple candidates).
			TreeMap<Double, LinkedList<CandidateSequences>> weightPerCandidate = new TreeMap<Double, LinkedList<CandidateSequences>>();
			 
			// Insert candidate's weights in ordered list.
			for(CandidateSequences candidate: getCandidates()) {
				double candidateWeight = getWeightedFitnessForCandidate(candidate);
				
				if(!weightPerCandidate.containsKey(candidateWeight))
					weightPerCandidate.put(candidateWeight, new LinkedList<CandidateSequences>());
				
				weightPerCandidate.get(candidateWeight).add(candidate);
			}
			
			// Rank candidate
			int rank = 1;
			
			for(Double weight: weightPerCandidate.keySet()) {
				for(CandidateSequences candidate: weightPerCandidate.get(weight)) {
					ranksPerCandidate.put(candidate, rank);
					rank++;
				}
			}
			
			
			return ranksPerCandidate;
		}
		
		public HashMap<CandidateSequences, LinkedHashMap<Sequence, Double>> getFitnessForDataStructures() {
			LinkedHashMap<Sequence,Double> totalFitnessPerSequence = getTotalFitnessPerSequence();
			this.fitnessPerCandidate = new HashMap<CandidateSequences,LinkedHashMap<Sequence,Double>>();
			
			for(CandidateSequences candidate: getCandidates()) {
				fitnessPerCandidate.put(candidate, candidate.getFitness(totalFitnessPerSequence));
			}
			
			try {
				System.out.println(fitnessPerCandidate);
//				TimeUnit.SECONDS.sleep(2);
				printFitnessPerDataStructure();
//				TimeUnit.SECONDS.sleep(5);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				try {
					TimeUnit.SECONDS.sleep(15);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				e.printStackTrace();
			}
			
			return fitnessPerCandidate;
		}
		
		/**
		 * Gets sum of relative fitness for the candidate per sequence, 
		 * multiplies this fitness by the sequence's relative importance (frequency).
		 * 
		 * @param candidate
		 * @return
		 */
		public double getWeightedFitnessForCandidate(CandidateSequences candidate) {
			double weightedFitness = 0;
			
			for(Sequence seq: fitnessPerCandidate.get(candidate).keySet()) {
				weightedFitness += fitnessPerCandidate.get(candidate).get(seq) * seq.getFrequency();
			}
			
			return weightedFitness;
		}
		
		/**
		 * Print matrix per Candidate: Relative importance per Candidate and per Sequence
		 */
		public void printFitnessPerDataStructure() {
			int maxLength = 0;
			
			for(CandidateSequences candidate: fitnessPerCandidate.keySet()) {
				maxLength = Math.max(maxLength, candidate.toString().length());
			}
			
			// Should be 1 if normalized total sum.
			HashMap<Sequence, Double> totalFitnessPerSequence = new HashMap<Sequence, Double>();
			
			for(Sequence seq: getTotalFitnessPerSequence().keySet()) {
				System.out.println(seq);
				System.out.println(getTotalFitnessPerSequence().get(seq));
				
				totalFitnessPerSequence.put(seq, new Double(0));
			}
			
			// Per candidate print out fitness for each sequence, and weighted total fitness.
			for(CandidateSequences candidate: fitnessPerCandidate.keySet()) {
				String line = String.format("%" + maxLength + "s", candidate.toString()) + " ";
				
				LinkedHashMap<Sequence, Double> fitnessPerSequence = fitnessPerCandidate.get(candidate);			
				
				for(Sequence seq: fitnessPerSequence.keySet()) {
					line += String.format("%.4f", fitnessPerSequence.get(seq)) + "  ";
					totalFitnessPerSequence.put(seq, new Double(totalFitnessPerSequence.get(seq) + fitnessPerSequence.get(seq)));
				}
				
				line += String.format("w: %.4f", getWeightedFitnessForCandidate(candidate));
				line += String.format("   r: %d", getRanksForDataStructures().get(candidate));
				line += String.format("   QP size: %d", candidate.getSubscribedQueryPlans().size());
				
				System.out.println(line);		
			}
			
			// Print sum of fitness per sequence. (should be 1s).
			String line = String.format("%" + maxLength + "s", " ");
			
			for(Sequence seq: totalFitnessPerSequence.keySet()) {
				line += String.format("%.4f", totalFitnessPerSequence.get(seq)) + "  ";
			}
			
			System.out.println(line);
		}
		
		public LinkedHashMap<Sequence, Double> getTotalFitnessPerSequence() {
			LinkedHashMap<Sequence, Double> totalFitnessPerSequence = new LinkedHashMap<Sequence,Double>();
			
			
			for(Sequence seq: getSequences()) {
				totalFitnessPerSequence.put(seq, gen.existingQueryPlans.get(seq).getTotalFitness());
			}
			
			return totalFitnessPerSequence;
		}
		
		/**
		 * Prunes data structures which are regarded inefficient:
		 * - (1) Low weighted efficiency rank per Sequence - QueryPlans
		 * - (2) Non-essential (not contained in essential queryPlan).
		 */
		public void pruneDataStructures() {
			// Inverse - put Rank per Candidate
			HashMap<CandidateSequences, Integer> dataStructureRanks = getRanksForDataStructures();
			HashMap<Integer, CandidateSequences> ranksDataStructures = new HashMap<>();
			
			for(Map.Entry<CandidateSequences, Integer> entry : dataStructureRanks.entrySet()){
				ranksDataStructures.put(entry.getValue(), entry.getKey());
			}
			
			// Start at the lowest rank 1 and remove until sufficient documents have been pruned.
			int lowestRank = 1;
			
			while(getCandidates().size() > Config.MAX_NR_DOCUMENTS) {
				CandidateSequences candidate = ranksDataStructures.get(lowestRank);
				System.out.println(lowestRank);
				if(candidate==null || lowestRank > getCandidates().size() * 2)
					break;
				
				if(canRemoveDataStructure(candidate)) {
					pruneDocument(candidate);
					// Recalculate ranks
					beforePruning();
					
					dataStructureRanks = getRanksForDataStructures();
					for(Map.Entry<CandidateSequences, Integer> entry : dataStructureRanks.entrySet()){
						ranksDataStructures.put(entry.getValue(), entry.getKey());
					}
					
					if(getCandidates().size() <= 8) {
						gen.printQueryPlansPerSequence();
					}
					
					lowestRank=0;
				}
				
				lowestRank++;				
			}
		}
		
		/**
		 * Remove a document.
		 * 
		 * @param candidate
		 */
		private void pruneDocument(CandidateSequences candidate) {
			if(!canRemoveDataStructure(candidate))
				return;
			
			gen.candidates.remove(candidate);
			
			for(QueryPlan<CandidateSequences> qp: candidate.getSubscribedQueryPlans()) {
				gen.removeQueryPlan(qp);
			}
		}

		/**
		 * A data structure can be removed if it is non-essential by any of the sequences their current query plans.
		 * 
		 * @param candidate
		 * @return
		 */
		public boolean canRemoveDataStructure(CandidateSequences candidate) {
			if(candidate == null)
				return false;
			
			Set<QueryPlan<CandidateSequences>> affectedQueryPlans = candidate.getSubscribedQueryPlans();
			
			// Check per sequence if its bucket is greater than 1 to be able to remove this CS.
			// Sequence QP Bucket should have at least 1 QP if there is a reference by the CS.
			for(QueryPlan<CandidateSequences> qp: affectedQueryPlans) {
				if(existingQueryPlans.get(qp.getSequence()).size() == 1) {
					return false;
				}
				else if(existingQueryPlans.get(qp.getSequence()).size() < 1) {
					throw new IllegalStateException("A data structure has a subscribed query plan, which is not listed in the generator for the sequence");
				}
			}
			
			return true;
		}
		
		/**
		 * For each sequence: get its bucket.
		 * 
		 * Per query plan bucket size: only keep the X best query plans.
		 */
		public void pruneQueryPlans() {
			int removed = 0;
			
			for(Sequence seq: existingQueryPlans.keySet()) {
				List<QueryPlan<CandidateSequences>> toBeRemoved = 
						existingQueryPlans.get(seq).pruneEachBucketAndLeaveNrOfQps(Config.PRUNE_LEAVE_NR_PLANS_PER_BUCKET);
				
				removed += toBeRemoved.size();
				gen.removeQueryPlans(seq, toBeRemoved);
			}
			
			System.out.println("Removed QPs:" + removed);
		}
	}
	
	/**
	 * Stores query plans per size in buckets.
	 * 
	 * Each sequence has a big query plans bucket, that are sorted internally by size.
	 * 
	 * @author vincent
	 *
	 */
	private class QueryPlansBucket  {
		
		private HashMap<Integer, HashSet<QueryPlan<CandidateSequences>>> queryPlans = new HashMap<Integer, HashSet<QueryPlan<CandidateSequences>>>();
		
		// keep sorted list of keys of QP costs.
		//TODO: Fix something wrong!!
		private TreeMap<Integer, HashSet<QueryPlan<CandidateSequences>>> queryPlansByCost = new TreeMap<Integer, HashSet<QueryPlan<CandidateSequences>>>();
		
		/**
		 * Inserts QP in the correct bucket.
		 * Inserts QP in the general list ordered by cost.
		 * 
		 * @param qp
		 */
		public void addQueryPlan(QueryPlan<CandidateSequences> qp) {
			if(qp == null)
				return;
			
			// Insert QP in correct bucket
			if(!queryPlans.containsKey(Integer.valueOf(qp.size())))
					queryPlans.put(Integer.valueOf(qp.size()), new HashSet<QueryPlan<CandidateSequences>>());
			
			queryPlans.get(Integer.valueOf(qp.size())).add(qp);
			
			// Insert QP into rank
			if(!queryPlansByCost.containsKey(qp.getCost()))
				queryPlansByCost.put(qp.getCost(), new HashSet<QueryPlan<CandidateSequences>>());
			
			queryPlansByCost.get(qp.getCost()).add(qp);
		}
		
		/**
		 * Add multiple query plans.
		 * 
		 * @param qps
		 */
		public void addQueryPlans(Set<QueryPlan<CandidateSequences>> qps) {
			for(QueryPlan<CandidateSequences> qp: qps) {
				this.addQueryPlan(qp);
			}
		}
		
		/**
		 * Checks if any bucket contains the given query plan.
		 * 
		 * @param qp
		 * @return
		 */
		public boolean contains(QueryPlan<CandidateSequences> qp) {
			if(qp == null)
				return false;
			
			if(!queryPlans.containsKey(Integer.valueOf(qp.size())))
				return false;
			
			return queryPlans.get(Integer.valueOf(qp.size())).contains(qp);			
		}
		
		/**
		 * Remove QP from bucket size.
		 * Remove QP form ordered cost TreeMap.
		 * 
		 * @param qp
		 */
		public void removeQueryPlan(QueryPlan<CandidateSequences> qp) {
			if(qp == null)
				return;
			
			qp.unsubscribeToCandidates();
			
			queryPlans.get(Integer.valueOf(qp.size())).remove(qp);
			queryPlansByCost.get(qp.getCost()).remove(qp);
		}
		
		/**
		 * Remove multiple query plans.
		 * 
		 * @param qps
		 */
		public void removeQueryPlans(Collection<QueryPlan<CandidateSequences>> qps) {
			for(QueryPlan<CandidateSequences> qp: qps) {
				this.removeQueryPlan(qp);
			}
		}
		
		/**
		 * Gets all query plans in this bucket, regardless of size.
		 * 
		 * @return
		 */
		public Set<QueryPlan<CandidateSequences>> getQueryPlans() {
			TreeSet<QueryPlan<CandidateSequences>> allQueryPlans = new TreeSet<QueryPlan<CandidateSequences>>();
			
			for(Integer sizeKey: queryPlans.keySet()) {
				allQueryPlans.addAll(queryPlans.get(sizeKey));
			}
			
			return allQueryPlans;
		}
		
		/**
		 * Gets all query plans in of a certain size.
		 * 
		 * @return
		 */
		public Set<QueryPlan<CandidateSequences>> getQueryPlansOfSize(int size) {
			return queryPlans.get(Integer.valueOf(size));
		}
		
		/**
		 * Total number of query plans.
		 * 
		 * @return
		 */
		public int size() {
			int size = 0;
			
			for(Integer sizeKey: queryPlans.keySet()) {
				size += queryPlans.get(sizeKey).size();
			}
			
			return size;
		}
		
		/**
		 * Returns set of query plans of a certain size.
		 * @param size
		 * @return
		 */
		public HashSet<QueryPlan<CandidateSequences>> getQueryPlans(int size) {
			return queryPlans.get(Integer.valueOf(size));
		}
		
		public List<QueryPlan<CandidateSequences>> pruneEachBucketAndLeaveNrOfQps(int amountToLeave) {
			List<QueryPlan<CandidateSequences>> toBeRemovedQPs = new ArrayList<QueryPlan<CandidateSequences>>();
			
			for(Integer bucketSize: queryPlans.keySet()) {
				
				// first sort QPs by cost per buck
				HashSet<QueryPlan<CandidateSequences>> qps = queryPlans.get(bucketSize);
				Set<QueryPlan<CandidateSequences>> bestQPs = selectBestQueryPlans(amountToLeave, qps);
				
						
				// leave X best QPs
				for(QueryPlan<CandidateSequences> qp: queryPlans.get(bucketSize)) {
					if(!bestQPs.contains(qp)) {
						toBeRemovedQPs.add(qp);
					}
				}
				
				System.out.println("bucket size " + bucketSize);
				System.out.println("Original QPS: " + qps.size());
				System.out.println("To be removed:" + toBeRemovedQPs.size());
				
			}
			
			
			return toBeRemovedQPs;
		}
		
		public double getTotalFitness() {
			double fitness = 0;
			
			for(QueryPlan<CandidateSequences> qp: getQueryPlans()) {
				fitness += qp.getFitness();
			}
			
			return fitness;
		}
		
		/**
		 * Done every now and then before pruning.
		 */
		public void precalculateQueryPlanRanks() {
			// Cost in ascending order
			int totalRank = size();
			int rank = size();
			
			// Ordered keys in ascending order (ascending cost).
			// Cheapest QP first.
			for(int cost: queryPlansByCost.keySet()) {
				for(QueryPlan<CandidateSequences> qp: queryPlansByCost.get(cost)) {
					qp.setRank(rank);
					qp.setFitness(rank, totalRank);
//					System.out.println(qp);
//					System.out.println(rank);
					
					rank--;
				}
				
				
			}
		}
		
		/**
		 * Selects X best query plans by cost.
		 * 
		 * @param amount
		 * @return
		 */
		private Set<QueryPlan<CandidateSequences>> selectBestQueryPlans(int amount, Set<QueryPlan<CandidateSequences>> qps) {
			// Keep same indexes between these structures.
			LinkedList<Integer> bestCosts = new LinkedList<Integer>();
			LinkedList<QueryPlan<CandidateSequences>> bestQPs = new LinkedList<QueryPlan<CandidateSequences>>();
			
			
			// Per QP check if its better than any of the existing X QPS.
			for(QueryPlan<CandidateSequences> qp: qps) {
				int cost = qp.getCost();
				
				if(bestQPs.size() < amount) {
					bestCosts.add(cost);
					bestQPs.add(qp);
				}
				else if(cost < Collections.max(bestCosts)) {
					int index = bestCosts.indexOf(Collections.max(bestCosts));
					bestCosts.remove(index);
					bestQPs.remove(index);
					
					bestCosts.add(cost);
					bestQPs.add(qp);
				}
			}
			
			return new HashSet<QueryPlan<CandidateSequences>>(bestQPs);
		}
	}
	
}

