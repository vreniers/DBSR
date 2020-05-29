package dbsr.cost;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;

import dbsr.workload.Sequence;

public interface RankCandidate {

	/**
	 * A data structure ranks itself, based on the (absolute) total rank per sequence given.
	 * 
	 * Returns its relative rank per sequence.
	 * 
	 * @param totalRankPerSequence
	 * @return
	 */
	public LinkedHashMap<Sequence, Double> getRank(LinkedHashMap<Sequence, BigInteger> totalRankPerSequence);
	
	public LinkedHashMap<Sequence, Double> getFitness(LinkedHashMap<Sequence, Double> totalFitnessPerSequence);
}
