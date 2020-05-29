package dbsr.candidate.generator.optimizations;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;

import dbsr.candidate.Candidate;
import dbsr.config.Config;
import dbsr.workload.QueryPlan;

/**
 * Notifies entire candidate tree and its associated query plans of the existence of a new data structure.
 * 
 * Manages notifier threads.
 * 
 * @author vincent
 */
public class QueryPlanNotifier<T extends Candidate<T>> {

	private final T candidate;
	
	private final Set<Candidate<T>> candidateTree;
	
	protected final HashSet<QueryPlan<T>> generatedQPs;
	
	private final LinkedHashSet<QueryPlan<T>> queryPlans = new LinkedHashSet<QueryPlan<T>>();
	
	private final Set<QueryPlan<T>> newQPs = new HashSet<QueryPlan<T>>();
	
	public QueryPlanNotifier(T candidate, HashSet<QueryPlan<T>> generatedQPs) {
		this.generatedQPs = generatedQPs;
		
		// collect candidates (into set)
		this.candidate = candidate;
		this.candidateTree = candidate.getCandidateTree();
		
//		System.out.println("candidate: " + candidate);
//		System.out.println("candidate tree size: " + candidateTree.size());
		
		// merge all query plans
		for(Candidate<T> cand: candidateTree) {
			queryPlans.addAll(cand.getSubscribedQueryPlans());
		}
		
//		System.out.println(" query plan size: " + queryPlans.size());
		
		this.startThreads();
	}
	
	private void startThreads() {
		// set number of threads
		int numberOfThreads = Math.max(1, queryPlans.size() / 500);
		numberOfThreads = Math.min(numberOfThreads, Config.MAX_THREADS);
		
		// divide set of query plans across MAX threads
		Iterable<List<QueryPlan<T>>> queryPlanPartitions = Iterables.partition(queryPlans, (queryPlans.size() / numberOfThreads) +1);
		Iterator<List<QueryPlan<T>>> iterator = queryPlanPartitions.iterator();
		
		// start MAX threads
		ArrayList<NotifierThread> notifiers = new ArrayList<NotifierThread>();
		
		for(int i=0; i< numberOfThreads; i++) {
			List<QueryPlan<T>> list = iterator.next();
			NotifierThread notifier = new NotifierThread(i, list.iterator());
			notifiers.add(notifier);
			
//			System.out.println(" new thread ");
			notifier.start();
		}
		
		// wait till threads are finished.
		for(Thread t: notifiers) {
			try {
				t.join();
//				System.out.println(" threads finished");
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// collect results
		for(NotifierThread t: notifiers) {
			newQPs.addAll(t.getNewQPs());
		}
		
//		System.out.println("new qps size:" + newQPs.size());
	}
	
	public Set<QueryPlan<T>> getNewQueryPlans() {
		return this.newQPs;
	}
	
	/**
	 * Notifier thread
	 * 
	 * @author vincent
	 */
	public class NotifierThread extends Thread {
		
		/**
		 * Used to allocate the range of query plans to process.
		 */
		private final Iterator<QueryPlan<T>> iterator;
		
		private final int threadNr;
		
		private final Set<QueryPlan<T>> newQPs = new HashSet<QueryPlan<T>>();
		
		public NotifierThread(int threadNr, Iterator<QueryPlan<T>> iterator) {
			this.iterator = iterator;
			this.threadNr = threadNr;
		}
		
		public void run() {
			while(iterator.hasNext()) {
				QueryPlan<T> qp = iterator.next();
				
				Set<QueryPlan<T>> newPlans = qp.notifyNewCandidate(candidate);
				newPlans.removeAll(generatedQPs);
				
				newQPs.addAll(newPlans);
			}
		}
			
		public Set<QueryPlan<T>> getNewQPs() {
			return this.newQPs;
		}
		
	}

}
