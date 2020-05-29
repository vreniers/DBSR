package dbsr.model;

public interface TreeOverlap<T> {

	/**
	 * Checks if the elements of this tree is a subset of the given tree.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean isSubSetOf(T otherCandidate);
	
	/**
	 * TODO
	 * Checks if this tree is a subset of the given tree.
	 * The elements have to be connected in the right order.
	 * 
	 * For example: Bids -> Items -> Users (Y)
	 * Users(Y) is not a SubTree. Since some users may not have any bids.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean isSubTreeOf(T otherCandidate);
	
	/**
	 * Returns the part of this tree which overlaps with the given tree.
	 * The elements have to be connected.
	 * 
	 */
	public T getSubTreeOf(T otherCandidate);
	
	/**
	 * Checks if given tree is connected at the bottom of this tree.
	 * The trees do not overlap.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean connectedAtTopOf(T tree);
	
	/**
	 * Checks if the bottom of the given tree is connected to the top of this tree.
	 * The trees do not overlap.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean connectedAtBottomOf(T tree);
	
	/**
	 * Either connected at the top or at the bottom.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean connectedTo(T otherCandidate);
	
	/**
	 * Checks if this tree overlaps partially at the top of the given tree.
	 * 
	 * @param otherCandidate
	 * @return
	 */
	public boolean overlapsAtTopOf(T otherCandidate);
	
	/**
	 * Checks if this tree overlaps partially at the bottom of the given tree.
	 * @param otherCandidate
	 * @return
	 */
	public boolean overlapsAtBottomOf(T otherCandidate);
	
}
