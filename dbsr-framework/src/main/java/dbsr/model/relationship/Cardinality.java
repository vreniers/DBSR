package dbsr.model.relationship;

/**
 * Now at least 1-1, future work includes distribution.
 * [0,1] etc.
 * 
 * Users -> Items: 1..N or 0..N
 * 
 * Currently: 3..6 (?) (3 users have 6 items)?
 * 
 * @author vincent
 */
public class Cardinality {
	private final int sourceFreq;
	
	private final int targetFreq;
	
	//TODO cardinalities that are optional?
	public Cardinality(int sourceFreq, int targetFreq) {
		if(sourceFreq <1 || targetFreq <1) {
			throw new IllegalArgumentException("Frequency below 1 not yet supported.");
		}
		
		if(sourceFreq < 1)
			sourceFreq = 0;
		if(targetFreq < 1)
			targetFreq = 1;
		
		this.sourceFreq = sourceFreq;
		this.targetFreq = targetFreq;
	}
	
	public int getSourceFreq() {
		return this.sourceFreq;
	}
	
	public int getTargetFreq() {
		return this.targetFreq;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + sourceFreq;
		result = prime * result + targetFreq;
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
		Cardinality other = (Cardinality) obj;
		if (sourceFreq != other.sourceFreq)
			return false;
		if (targetFreq != other.targetFreq)
			return false;
		return true;
	}
	
	
}