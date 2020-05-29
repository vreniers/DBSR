package dbsr.workload;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import dbsr.model.Entity;
import dbsr.model.tree.EntityTree;
import dbsr.workload.query.Query;

/**
 * Shows in what manner entities are traversed and with what cardinality.
 * Collects multiple sequences as long as they belong together.
 * 
 * @author vincent
 * 
 * TODO: Unused class?
 */
public class SequenceGraph {
	
	private final List<Sequence> sequences = new ArrayList<Sequence>();
	
	/**
	 * TODO unused?
	 */
	private final Map<Sequence, LinkedList<EntityTree>> sequence_paths =
			new HashMap<Sequence, LinkedList<EntityTree>>();
	
	private EntityTree graph;
	
	// Tree structure or paths...by level? and weight to edges?
	
	public SequenceGraph(Sequence seq) {
		Entity root = seq.getPath().get(0);
		graph = new EntityTree(root);
		
		addSequence(seq);
	}
	
	/**
	 * Returns a list of points in the tree which the sequence traverses.
	 * 
	 * @param seq
	 * @return
	 */
	public void addSequence(Sequence seq) {
		this.sequences.add(seq);
		// use query instead of entity? add query at node? -> Collection of Queries per Node.
		// Determines attributes selected? And size of candidates? at attribute-level...
		LinkedList<Query> path = seq.getSequence();
		Iterator<Query> iterator = path.iterator();
		EntityTree node = graph;
		
		while(iterator.hasNext()) {
			Query query = iterator.next();
			
			if(node.getNode().equals(query.getEntity()))
				continue;
			
			if(!node.getChildren().contains(query.getEntity())){
				EntityTree child = node.addChild(query.getEntity());
				node = child;
			}
		}
	}
	
	/**
	 * Returns a new entity tree from a given sequence.
	 * @param seq
	 * @return
	 */
	public static EntityTree getTree(Sequence seq) {
		LinkedList<Query> path = seq.getSequence();
		Iterator<Query> iterator = path.iterator();
		EntityTree node = null;
		
		while(iterator.hasNext()) {
			Query query = iterator.next();
			
			if(node == null)
				node = new EntityTree(query.getEntity());
			
			else if(!node.getChildren().contains(query.getEntity())){
				EntityTree child = node.addChild(query.getEntity());
				node = child;
			}
		}
		
		return node;
	}
	
	public EntityTree getGraph() {
		return this.graph;
	}
	 
}
