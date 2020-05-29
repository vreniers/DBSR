package dbsr.workload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import dbsr.model.Entity;
import dbsr.model.tree.EntityTree;
import dbsr.workload.query.Query;

/**
 * Combination of queries which are related, and executed sequentially in practice.
 * Sequence traverses 1-1, 1-n relations, and gets associated records.
 * 
 * Caution when sequence is cyclic! Order is important!
 * => Establish directed graph?
 * 
 * 
 * SELECT *
 *		FROM employee JOIN department
 *			ON employee.DepartmentID = department.DepartmentID;
 * 
 * 1) SELECT users.*, regions.info, regions.id, users2.* FROM users WHERE users.cat=X 
 * 		JOIN regions ON regions.id = users.region_id
 * 		JOIN users AS users2 ON users2.region_id = region.id   --- frequency ~ 10
 * 
 * ==
 * res1= SELECT users.* FROM users WHERE users.cat=X
 * res2= SELECT regions.* FROM regions WHERE regions.id=/IN(res1.region_id) OR x,y,z...
 * res3= SELECT users.* FROM users WHERE users.region_id=/IN(res2.id) OR x,y,z,...
 * 
 * Same table join. (Halt condition?)
 * -> Depends on WHERE clause.
 * -> In essentie gewoon kijken welke relaties getraversed worden en impact berekenen adhv cardinaliteit, en
 * de gekozen data velden. (size)
 * 
 * 2) SELECT users.*, regions.info FROM users JOIN users.region_id = regions.id
 * 		... JOIN users.region_id => redundant
 * 
 * 3) SELECT users.* FROM users WHERE id=3
 * 4) SELECT users.* FROM users WHERE users.cat=4 AND users.company_id=20
 * 5) SELECT users.* FROM users WHERE age=25
 * 
 * -> How to represent?
 * 
 * => Data model options: How?
 * => Costs?
 * 
 * -> Hoe verlopen queries? Welke relaties horen samen? 
 * -> Collapsen van relaties onafhv. van query workload eerst?
 * -> Nadien schrappen op basis van workload.
 * 
 * SELECT p.Name, v.Name FROM Production.Product p 
 * 	JOIN Purchasing.ProductVendor pv
 * 		ON p.ProductID = pv.ProductID
 *  JOIN Purchasing.Vendor v
 *  	ON pv.BusinessEntityID = v.BusinessEntityID
 *  WHERE ProductSubcategoryID = 15
 *  
 * @author vincent
 */
public class Sequence {
	
	/**
	 * If a query occurs multiple times.
	 */
	private final List<Query> cyclicElements = new ArrayList<Query>();
	
	//TODO Aren't all sequences read?
	public enum SequenceType{
		READ, WRITE;
	}

	private final LinkedList<Query> sequence = new LinkedList<Query>();
	
	private final SequenceType type;
	
	/**
	 * Frequency this sequence occurs.
	 */
	private final int frequency;
	
	public Sequence(LinkedList<Query> sequence, SequenceType type) {
		this(sequence, type, 1);
	}
	
	public Sequence(LinkedList<Query> sequence, SequenceType type, int frequency) {
		for(Query query: sequence)
			this.addQuery(query);
		
		this.type = type;
		this.frequency = frequency;
	} 
	
	public Sequence() {
		this(1);
	}
	
	public Sequence(int frequency) {
		this.type = SequenceType.READ;
		this.frequency = frequency;
	}

	public LinkedList<Query> getSequence() {
		return sequence;
	}
	
	public int getFrequency() {
		return this.frequency;
	}
	
	/**
	 * Returns the overlap between two sets of sequences.
	 * 
	 * @param set1
	 * @param set2
	 * @return
	 */
	public static List<Sequence> overlapBetween(List<Sequence> set1, List<Sequence> set2) {
		ArrayList<Sequence> setOneCopy = new ArrayList<Sequence>(set1);
		ArrayList<Sequence> setTwoCopy = new ArrayList<Sequence>(set2);
		
		setOneCopy.retainAll(setTwoCopy);
		return setOneCopy;
	}
	
	/**
	 * TODO Verify whether query is actually possible.
	 * Relationship must exist between former entities(?).
	 * 
	 * @param query
	 */
	public void addQuery(Query query) {
		if(this.sequence.contains(query)) {
			this.cyclicElements.add(query);
		}
		
		this.sequence.add(query);
		query.addSequence(this);
	}
	
	public SequenceType getType() {
		return this.type;
	}
	
	/**
	 * @return Returns the path of traversed entities.
	 */
	public LinkedList<Entity> getPath() {
		LinkedList<Entity> path = new LinkedList<Entity>();
		
		for(Query query: sequence) {
			path.add(query.getEntity());
		}
		
		return path;
	}
	
	/**
	 * @return Returns the path of executed queries per entity.
	 * 
	 * TODO: Make copy(?)!
	 */
	public LinkedList<Query> getQueryPath() {
		return this.sequence;
	}
	
	/**
	 * Creates an entity tree from the queries available.
	 * 
	 * @return
	 */
	public EntityTree getEntityTree() {
		return EntityTree.createEntityTree(getQueryPath());
	}

	@Override
	public String toString() {
		return "Sequence [sequence=" + sequence + ", type=" + type + "]";
	}
	
	/**
	 * Checks if this sequence is *PARTIALLY* present from the leaf node to to the top.
	 * And the from the other tree's top to the bottom.
	 * 
	 * The queries have to occur in the correct order.
	 * 
	 * TRICKY Because: Q1 -> .. -> Q2 -> Q3; Tree can contain order, maybe just not directly connected (?).
	 * 
	 * TODO: Should this function check if Seq is completely present? Or just partially.
	 * TODO: Whatif there are multiple options? 
	 * 
	 * @param leaf
	 * @param tree
	 * @return
	 */
	public boolean isSequenceConnectedBetween(EntityTree leaf, EntityTree tree) {
		// Check
		if(!tree.hasQueryOfSequence(this))
			return false;
		
		// Check valid leaf -> top query order of this sequence.
		LinkedList<Query> queryOrder = new LinkedList<Query>();
		queryOrder.add(leaf.getQueryOfSequence(this));
		
		// Add remaining queries to top.
		EntityTree leafNode = leaf;
		
		while(leafNode.hasParent()) {
			queryOrder.addFirst(leafNode.getParent().getQueryOfSequence(this));
			leafNode = leafNode.getParent();
		}
		
		if(!contains(queryOrder)) 
			return false;
		
		// Nodes can be skipped thats why we check the QP from Top to Node.
		if(!leaf.hasQueryPathToParent(queryOrder)) {
			return false;
		}
		
		// Add top query
		queryOrder.add(tree.getQueryOfSequence(this));
		
		if(!contains(queryOrder)) 
			return false;
		
		return true;
	}
	
//	/**
//	 * Checks if this sequence can be partially queried from the leaf node to to the top.
//	 * And the from the other tree's top to the bottom.
//	 * 
//	 * The queries have to occur in the correct order; represented by e.g. nodes that can answer the Q.
//	 * Nodes do not have to be directly connected in the order.
//	 * 
//	 * TODO: Should this function check if Seq is completely present? Or just partially.
//	 * 
//	 * @param leaf
//	 * @param tree
//	 * @return
//	 */
//	public boolean isSequenceLooselyConnectedBetween(EntityTree leaf, EntityTree tree) {
//		// Check
//		List<Query> possibleSeqQueries = tree.getQueriesPossibleForSequence(this);
//		
//		if(possibleSeqQueries.isEmpty())
//			return false;
//		
//
//		// Check valid leaf -> top query order of this sequence.
//		LinkedList<List<Query>> queryPossibilitiesInOrder = new LinkedList<List<Query>>();
//		queryPossibilitiesInOrder.add(possibleSeqQueries);
//		
//		// Add remaining queries to top.
//		EntityTree leafNode = leaf;
//		
//		while(leafNode.hasParent()) {
//			queryPossibilitiesInOrder.addFirst(leafNode.getParent().getQueriesPossibleForSequence(this));
//			leafNode = leafNode.getParent();
//		}
//		
//		LinkedList<LinkedList<Query>> qryPaths = getPossibleQueryPaths(queryPossibilitiesInOrder);
//		
////		if(!leaf.hasQueryPathToParent(queryOrder)) {
////			return false;
////		}
////		
////		// Add top query
////		queryOrder.add(tree.getQueryOfSequence(this));
////		
////		if(!contains(queryOrder)) 
////			return false;
//		
//		return true;
//	}
//	
//	/**
//	 * Given is a linked list of lists which choices of a Sequence query.
//	 * 
//	 * @param queryPossibilitiesInOrder
//	 * @return
//	 */
//	private LinkedList<LinkedList<Query>> getPossibleQueryPaths(LinkedList<List<Query>> queryPossibilitiesInOrder) {
//		LinkedList<LinkedList<Query>> qryPossibilities = new LinkedList<LinkedList<Query>>();
//		
//		for(List<Query> queryStep: queryPossibilitiesInOrder) {
//			for(Query queryChoice: queryStep) {
//				
//			}
//		}
//		return null;
//	}

	/**
	 * Returns if a query occurs multiple times in the sequence.
	 * 
	 * @return
	 */
	public boolean isCyclic() {
		return this.cyclicElements.size() > 0;
	}
	
	private List<Query> getCyclicElements() {
		return this.cyclicElements;
	}
	
	/**
	 * Counts the number of occurences for a given query in this sequence.
	 * 
	 * @param query
	 * @return
	 */
	public int countQuery(Query query) {
		int count = 0;
		
		for(Query qry: sequence) {
			if(qry.equals(query))
				count++;
		}
		
		return count;
	}
	
	/**
	 * Returns true if the given set of queries is part of the sequence.
	 * 
	 * @param queries
	 * @return
	 */
	public boolean contains(LinkedList<Query> queries) {
		return Collections.indexOfSubList(sequence, queries) != -1;
	}
	
	public boolean containsQuery(Query query) {
		return (sequence.contains(query));
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((sequence == null) ? 0 : sequence.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		Sequence other = (Sequence) obj;
		if (sequence == null) {
			if (other.sequence != null)
				return false;
		} else if (!sequence.equals(other.sequence))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
	/**
	 * Get all the associated fields per table.
	 * 
	 * TODO: Work on whether or not this is the best place to pass the model! 
	 * 
	 * Perhaps, only populate the Query if they have star operator, by correct columns!
	 * @return
	 */
//	private HashMap<String, Set<Field>> getAffectedFields() {
//		HashMap<String, Set<Field>> affectedAttributes = new HashMap<String, Set<Field>>();
//		
//		Iterator<Query> iterator = sequence.iterator();
//		
//		while(iterator.hasNext()) {
//			Query query = iterator.next();
//			
//			if(!affectedAttributes.containsKey(query.getTable().getName())) {
//				affectedAttributes.put(query.getTable().getName(), new HashSet<Field>());
//			}
//			
//			affectedAttributes.get(query.getTable().getName()).addAll(query.getAffectedFields());
//		}
//		
//		return affectedAttributes;
//	}
//	
	
	
}
