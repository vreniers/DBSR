package dbsr.candidate;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;

import dbsr.model.tree.EntityTree;
import dbsr.workload.Sequence;

/** 
 * Representation of a candidate collection.
 * 
 * Parent root entity -> children -> children etc....
 * 
 * Has a certain cost for associated queries.
 * 
 * How to take attributes into account for candidate? -> Refer to a query.
 * => Combination of individual query solutions?
 * 
 * @author vincent
 *
 */
public class CandidateSequence extends Candidate<CandidateSequence> {
	
	private final Sequence sequence;
	
	public CandidateSequence(EntityTree candidate, Sequence sequence) {
		super(candidate);
		
		this.sequence = sequence;
	}
	
	public CandidateSequence(EntityTree candidate, Sequence sequence, CandidateSequence leftMerge, CandidateSequence rightMerge) {
		super(candidate, leftMerge, rightMerge);
		
		this.sequence = sequence;
	}
	
	public Sequence getSequence() {
		return this.sequence;
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
	 * The new candidate has a list of subscribed query plans which can use the candidate.
	 * 
	 * @param otherCandidate Candidate to merge with this candidate.
	 * 		  OtherCandidate should be more right in the tree than this candidate.
	 * 
	 * TODO: Check ordering
	 * 
	 * @return
	 */
	@Override
	public CandidateSequence merge(CandidateSequence otherCandidate) {
		if(!canMerge(otherCandidate))
			return this;
		
		EntityTree newTree = this.entities.getTopParent().clone();
		EntityTree otherTree = otherCandidate.getCandidate().getTopParent().clone();
		
		EntityTree bottom = newTree.getRandomLeaf();
		
		if(otherTree.getNode().hasRelationshipWith(bottom.getNode())) {
			bottom.addChild(otherTree);
		}
		else if(otherTree.getNode().equals(bottom.getNode())) {
			bottom.addChildren(otherTree.getChildren());
		}
		
		// Merge the list of new relevant subscribed query plans (which can use the candidate).
		CandidateSequence candidate = new CandidateSequence(newTree, sequence, this, otherCandidate);
		
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
	public boolean canMerge(CandidateSequence otherCandidate) {
		return connectedAtTopOf(otherCandidate);
	}
	

	/**
	 * Checks whether two candidates are connected, and thus part of the same sequence.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean connectedTo(CandidateSequence otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!otherCandidate.getSequence().equals(sequence))
			return false;
		
		return otherCandidate.getCandidate().connectedTo(getCandidate());
	}
	
	@Override
	public CandidateSequence getSubTreeOf(CandidateSequence otherCandidate) {
		EntityTree tree = getCandidate().getSubTreeOf(otherCandidate.getCandidate());
		
		return new CandidateSequence(tree, sequence);
	}
	
	@Override
	public boolean connectedAtTopOf(CandidateSequence otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!otherCandidate.getSequence().equals(sequence))
			return false;
		
		return getCandidate().connectedAtTopOf(otherCandidate.getCandidate());
	}
	
	@Override
	public boolean connectedAtBottomOf(CandidateSequence otherCandidate) {
		if(otherCandidate == null)
			return false;
		
		if(!otherCandidate.getSequence().equals(sequence))
			return false;
		
		return getCandidate().connectedAtBottomOf(otherCandidate.getCandidate());
	}
	
	@Override
	public LinkedHashMap<Sequence, Double> getRank(LinkedHashMap<Sequence, BigInteger> totalRankPerSequence) {
		// To be implemented, probably not necessary for this Class though.
		throw new IllegalArgumentException("TO BE IMPLEMENTED");
	}

	@Override
	public LinkedHashMap<Sequence, Double> getFitness(LinkedHashMap<Sequence, Double> totalFitnessPerSequence) {
		// TODO Auto-generated method stub
		return null;
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
