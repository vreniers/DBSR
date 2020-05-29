package dbsr.cost;

public interface Rank<T> {
	
	public T getRank();
	
	public void setRank(T rank);
	
	public void setFitness(T currentRank, T seqTotalRank);
	
	public double getFitness();
	
}
