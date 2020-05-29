package dbsr.cost;

import java.util.List;
import java.util.Set;

import dbsr.candidate.Candidate;
import dbsr.model.Field;
import dbsr.model.tree.EntityTree;
import dbsr.workload.query.Query;

/**
 * Cost for reading an entire entity tree (on average, taking into account field costs and cardinalities at each level of nesting.)
 * 
 * Careful: AN EntityTree already counts how many time it occurs in the parent!!
 * 
 * @author vincent
 * 
 * TODO: Untested
 *
 */
public class CostCalculatorEntityTree implements Cost {

	private final EntityTree entityTree;
	
	public CostCalculatorEntityTree(EntityTree entityTree) {
		this.entityTree = entityTree;
	}
	
	@Override
	public int getCost() {
		// Start with the cost of the parent
		int cost = 0;
		
		Set<Field> fields = entityTree.getAffectedFields();
		cost += Field.getTotalCostOfMultipleFields(fields);	
		
		// Recursively calculate the cost of the children.
		for(EntityTree child: entityTree.getChildren()) {
			
			cost += child.getNodeFrequencyWithParent() * child.getCost();		
			
		}
		
		return cost;
	}

}
