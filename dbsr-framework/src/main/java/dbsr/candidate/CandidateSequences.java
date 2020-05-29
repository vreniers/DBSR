package dbsr.candidate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;

/** 
 * Representation of a candidate collection.
 * 
 * This candidate can represent multiple sequences.
 * 
 * @author vincent
 *
 */
public class CandidateSequences extends Candidate<CandidateSequences> {
	
	public CandidateSequences(EntityTree candidate) {
		super(candidate);
	}
	
	public CandidateSequences(EntityTree candidate,CandidateSequences leftMerge, CandidateSequences rightMerge) {
		super(candidate, leftMerge, rightMerge);
	}
	
	/**
	 * Get sequences that make use of this candidate via a query plan.
	 * 
	 * @return
	 */
	public Set<Sequence> getSequences() {
		Set<Sequence> sequences = new HashSet<Sequence>();
		
		for(QueryPlan<CandidateSequences> qp: getSubscribedQueryPlans()) {
			sequences.add(qp.getSequence());
		}
		
		return new HashSet<Sequence>(sequences);
	}
	
	/**
	 * Merges this candidate with a related candidate and creates a new structure.
	 * 
	 * @return
	 */
	@Override
	public CandidateSequences merge(CandidateSequences otherCandidate) {
		if(!canMerge(otherCandidate))
			return (CandidateSequences) this;
		
		EntityTree newTree;
		EntityTree otherTree;
		
		if(this.entities.getTopParent().canMergeInto(otherCandidate.getCandidate().getTopParent())) {
			newTree = this.entities.getTopParent().clone();
			otherTree = otherCandidate.getCandidate().getTopParent().clone();
		}else {
			newTree = otherCandidate.getCandidate().getTopParent().clone();;
			otherTree = this.entities.getTopParent().clone();
		}
		
		newTree.mergeInto(otherTree);
		
		Set<Sequence> sequences = new HashSet<Sequence>();
		sequences.addAll(getSequences());
		sequences.addAll(otherCandidate.getSequences());
		
		// Merge the list of new relevant subscribed query plans (which can use the candidate).
		CandidateSequences candidate = new CandidateSequences(newTree, this, otherCandidate);
		
		return candidate;
	}
	
	/**
	 * Determines if two candidate collections can be merged.
	 * For example:
	 * 1) [...,E1] + [E1,...] 
	 * 2) [...,E2] + [E3,...] and E2 and E3 are related.
	 * 
	 * OtherCandidate is connected by its top at the bottom of this candidate.
	 * No overlap between candidates.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean canMerge(CandidateSequences otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		return containsSubsetOfSequences(otherCandidate) && 
				getCandidate().canMerge(otherCandidate.getCandidate());
	}
	
	/**
	 * Checks if there is at least an overlap in sequences between the candidates to merge.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean containsSubsetOfSequences(CandidateSequences otherCandidate) {
		for(Sequence seq: getSequences()) {
			if(otherCandidate.getSequences().contains(seq))
				return true;
		}
		
		return false;
	}

	/**
	 * Checks whether two candidates are connected.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean connectedTo(CandidateSequences otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!containsSubsetOfSequences(otherCandidate))
			return false;
		
		return otherCandidate.getCandidate().connectedTo(getCandidate());
	}
	
	@Override
	public boolean connectedAtTopOf(CandidateSequences otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!containsSubsetOfSequences(otherCandidate))
			return false;
		
		return getCandidate().connectedAtTopOf(otherCandidate.getCandidate());
	}
	
	@Override
	public boolean connectedAtBottomOf(CandidateSequences otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!containsSubsetOfSequences(otherCandidate))
			return false;		
		
		return getCandidate().connectedAtBottomOf(otherCandidate.getCandidate());
	}
	
	@Override
	public CandidateSequences getSubTreeOf(CandidateSequences otherCandidate) {
		EntityTree tree = getCandidate().getSubTreeOf(otherCandidate.getCandidate());
		
		return new CandidateSequences(tree);
	}

	/**
	 * Input: total rank per sequence.
	 * 
	 * Output: calculate relative importance of query plans (subscribed) per sequence.
	 * 
	 * [ R1 (seq1), R2 (seq2), ... ]
	 * 
	 */
	@Override
	public LinkedHashMap<Sequence, Double> getRank(LinkedHashMap<Sequence, BigInteger> totalRankPerSequence) {
		LinkedHashMap<Sequence, Double> relativeRankPerSequence = new LinkedHashMap<Sequence, Double>();
		Set<QueryPlan<CandidateSequences>> subscribedQPs = getSubscribedQueryPlans();		
		
		//TODO: New fitness score set in QP
		//TODO: only N is now returned in totalRankPerSequence
		//... fix the rest of this code.
		
		// Insert zeroes in order first.
		for(Sequence seq: totalRankPerSequence.keySet())
			relativeRankPerSequence.put(seq, new Double(0));
		
		// Get total rank of subscribed query plans per sequence.
		for(QueryPlan<CandidateSequences> qp: subscribedQPs) {
			// Candidate may appear multiple times in the same query plan.
			Double relativeRankInQP = ((double) (Collections.frequency(qp.getCandidatesPlan(), this) * qp.getFitness())) / qp.getCandidatesPlan().size();
			Double relativeRankForSequence = relativeRankInQP + relativeRankPerSequence.get(qp.getSequence());
			relativeRankPerSequence.put(qp.getSequence(), relativeRankForSequence);		
		}
		
		// Normalize
		for(Sequence seq: relativeRankPerSequence.keySet()) {
			relativeRankPerSequence.put(seq, relativeRankPerSequence.get(seq) / totalRankPerSequence.get(seq).doubleValue());
		}
		
		return relativeRankPerSequence;
	}

	@Override
	public LinkedHashMap<Sequence, Double> getFitness(LinkedHashMap<Sequence, Double> totalFitnessPerSequence) {
		LinkedHashMap<Sequence, Double> relativeFitnessPerSequence = new LinkedHashMap<Sequence, Double>();
		Set<QueryPlan<CandidateSequences>> subscribedQPs = getSubscribedQueryPlans();		
		
		//TODO: New fitness score set in QP
		//TODO: only N is now returned in totalRankPerSequence
		//... fix the rest of this code.
		
		// Insert zeroes in order first.
		for(Sequence seq: totalFitnessPerSequence.keySet())
			relativeFitnessPerSequence.put(seq, new Double(0));
		
		// Get total rank of subscribed query plans per sequence.
		for(QueryPlan<CandidateSequences> qp: subscribedQPs) {
			// Candidate may appear multiple times in the same query plan.
			Double relativeRankInQP = ((double) (Collections.frequency(qp.getCandidatesPlan(), this) * qp.getFitness())) / qp.getCandidatesPlan().size();
			Double relativeRankForSequence = relativeRankInQP + relativeFitnessPerSequence.get(qp.getSequence());
			relativeFitnessPerSequence.put(qp.getSequence(), relativeRankForSequence);		
		}
		
		// Normalize
		for(Sequence seq: relativeFitnessPerSequence.keySet()) {
			relativeFitnessPerSequence.put(seq, relativeFitnessPerSequence.get(seq) / totalFitnessPerSequence.get(seq).doubleValue());
		}
		
		return relativeFitnessPerSequence;
	}

	

//	@Override
//	public int hashCode() {
//		final int prime = 31;
//		int result = super.hashCode();
////		result = prime * result + ((sequence == null) ? 0 : sequence.hashCode());
//		return result;
//	}

	// TODO: Does require sequence to be equal?
//	@Override
//	public boolean equals(Object obj) {
//		if (this == obj)
//			return true;
//		if (!super.equals(obj))
//			return false;
//		if (getClass() != obj.getClass())
//			return false;
//		CandidateSequence other = (CandidateSequence) obj;
//		if (sequence == null) {
//			if (other.sequence != null)
//				return false;
//		} else if (!sequence.equals(other.sequence))
//			return false;
//		return true;
//	}
	
	
}
