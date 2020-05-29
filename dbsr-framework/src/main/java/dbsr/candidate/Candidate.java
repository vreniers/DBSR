package dbsr.candidate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import dbsr.cost.RankCandidate;
import dbsr.model.TreeOverlap;
import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;

/** 
 * Representation of a candidate collection.
 * 
 * Parent root entity -> children -> children etc....
 * 
 * Has a certain cost for associated queries.
 * 
 * 1) First, start denormalized(?).
 * 2) Combine entitities.
 * 3) Combine new solutions.
 * 4) Halt when no more new solutions.
 * 
 * How to take attributes into account for candidate? -> Refer to a query.
 * => Combination of individual query solutions?
 * 
 * @author vincent
 *
 */
public abstract class Candidate<T extends Candidate<T>> implements TreeOverlap<T>, RankCandidate {
	
	protected EntityTree entities;
	
	private final HashSet<QueryPlan<T>> subscribers = new HashSet<QueryPlan<T>>();
	
	private final Candidate<T> leftMerge;
	
	private final Candidate<T> rightMerge;
	
	/**
	 * Initializes the candidate with a subset of entities in a tree.
	 * 
	 * @param candidate
	 */
	public Candidate(EntityTree candidate) {
		this(candidate , null , null);
	}
	
	public Candidate(EntityTree candidate, Candidate<T> leftMerge, Candidate<T> rightMerge) {
		this.entities = candidate;
		this.leftMerge = leftMerge;
		this.rightMerge = rightMerge;
	}
	
	/**
	 * @return Returns the candidate tree.
	 */
	public EntityTree getCandidate() {
		return this.entities;
	}
	
	public Candidate<T> getLeftMerge() {
		return this.leftMerge;
	}
	
	public Candidate<T> getRightMerge() {
		return this.rightMerge;
	}
	
	public Set<Candidate<T>> getCandidateTree() {
		HashSet<Candidate<T>> candidateTree = new HashSet<Candidate<T>>();
		
		if(!isRootCandidate()) {
			candidateTree.addAll(getLeftMerge().getCandidateTree());
			candidateTree.addAll(getRightMerge().getCandidateTree());
		}
		
		candidateTree.add(this);
		
		return candidateTree;
	}
	
	/**
	 * Returns the set of query plans making use of this data structure.
	 * 
	 */
	public Set<QueryPlan<T>> getSubscribedQueryPlans() {
		return new HashSet<QueryPlan<T>>(this.subscribers);
	}
	
	/**
	 * This candidate is not the result of merging two other candidates.
	 * 
	 * @return
	 */
	public boolean isRootCandidate() {
		return this.leftMerge == null && this.rightMerge == null;
	}
	
	public void addSubscribedQP(QueryPlan<T> qp) {
		this.addSubscribedQP(qp, 0);
	}
	
	/**
	 * Add a subscribed query plan if the level is 0, 
	 * and also notify the root structure of subscribed query plans at each level.
	 * 
	 * @param qp
	 * @param level
	 */
	protected void addSubscribedQP(QueryPlan<T> qp, int level) {		
		this.subscribers.add(qp);
	}
	
	public void addSubscribedQPs(Set<QueryPlan<T>> qps) {
		this.subscribers.addAll(qps);
	}
	
	public void removeSubscribedQP(QueryPlan<T> qp) {
		// Remove from root subscriber list.)
		this.subscribers.remove(qp);
	}
	
	
	/**
	 * Notifies all associated query plans of a new candidate.
	 * 
	 * TODO: If this was multi-threaded I think it would probably improve a lot.
	 * 
	 * @param candidate
	 */
	public Set<QueryPlan<T>> notifySubscribers(T candidate) {
		Set<QueryPlan<T>> newQPs = notifySubScribersNonRecursive(candidate);
			
		if(!isRootCandidate()) {
			newQPs.addAll(this.leftMerge.notifySubscribers(candidate));
			newQPs.addAll(this.rightMerge.notifySubscribers(candidate));
		}
		
		return newQPs;
	}
	
	/**
	 * Notifies all associated query plans of a new candidate only at this level.
	 */
	public Set<QueryPlan<T>> notifySubScribersNonRecursive(T candidate) {
		HashSet<QueryPlan<T>> newQPs = new HashSet<QueryPlan<T>>();
		
		for(QueryPlan<T> qp: subscribers) {
			newQPs.addAll(qp.notifyNewCandidate(candidate));
		}
		
		// LEAVE to Generator!
		//candidate.addSubscribedQPs(newQPs);
		
		return newQPs;
	}
	
	/**
	 * Merges this candidate with a related candidate and creates a new structure.
	 * 
	 * [C1, C2] + [C3, C4] = [C1,C2,C3,C4]
	 * [C1, C2] + [C2, C3] = [C1,C2,C3]
	 * [C1] + [C2, C3] = [C1,C2,C3]
	 * 
	 * Don't merge when no direct relationship available. 
	 * (Collapse later on) e.g. [C1,C2,C3] = [C1,C3] if nothing selected at C2.
	 * [C1] + [C3] = [C1,C3].
	 * 
	 * What-if
	 * TODO: [C2,C1] + [C2,C4] =? Not possible (yet?) or desired for a Sequence?
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public abstract T merge(T otherCandidate);
	
	public abstract boolean canMerge(T otherCandidate);
	
	@Override
	public boolean isSubSetOf(T otherCandidate) {
		return getCandidate().isSubSetOf(otherCandidate.getCandidate());
	}
	
	@Override
	public boolean isSubTreeOf(T otherCandidate) {
		return getCandidate().isSubTreeOf(otherCandidate.getCandidate());
	}
	
	@Override
	public boolean overlapsAtTopOf(T otherCandidate) {
		return getCandidate().overlapsAtTopOf(otherCandidate.getCandidate());
	}

	@Override
	public boolean overlapsAtBottomOf(T otherCandidate) {
		return getCandidate().overlapsAtBottomOf(otherCandidate.getCandidate());
	}
	
	/**
	 * [C1,C2,C3] = [C1,C3] when nothing selected at C2.
	 * 
	 * @return
	 */
	public boolean isCollapsable() {
		return false;
	}
	
	/**
	 * Remove unnecessary relationships. e.g.
	 * 
	 * [C1,C2,C3] => [C1,C3] when nothing selected or needed from C2.
	 */
	public void collapse() {
		if(!isCollapsable())
			return;
	}
	
	@Override
	public String toString() {
		return entities.toString();
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((entities == null) ? 0 : entities.hashCode());
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
		T other = (T) obj;
		if (entities == null) {
			if (other.entities != null)
				return false;
		} else if (!entities.equalsEntireTree(other.entities))
			return false;
		
		return true;
	}
}
