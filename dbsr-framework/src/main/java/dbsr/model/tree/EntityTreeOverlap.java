package dbsr.model.tree;

import java.util.List;

/**
 * Wrapper class for matching EntityTree's and the size.
 * 
 * @author vincent
 */
public class EntityTreeOverlap {

	private final EntityTree partition;
	
	private final int size;
	
	/**
	 * @param size
	 * @param partition
	 */
	public EntityTreeOverlap(int size, EntityTree partition) {
		this.partition = partition;
		this.size = size;
	}
	
	public EntityTree getPartition() {
		return this.partition;
	}
	
	public int getSize() {
		return this.size;
	}
	
	/**
	 * Select largest overlap from a collection.
	 * 
	 * @param list
	 * @return
	 */
	public static EntityTreeOverlap retrieveLargestOverlap(List<EntityTreeOverlap> list) {
		if(list == null)
			return null;
		
		if(list.isEmpty())
			return null;
		
		EntityTreeOverlap best = null;
		
		for(EntityTreeOverlap overlap: list) {
			if(best == null )
				best = overlap;
			else if(best.getSize() > overlap.getSize())
				best = overlap;
		}
		
		return best;
	}
}
