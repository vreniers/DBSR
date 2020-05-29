package dbsr.model.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import dbsr.cost.Cost;
import dbsr.cost.CostCalculatorEntityTree;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.TreeOverlap;
import dbsr.model.relationship.Relationship;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;

/**
 * TreeNode for Entities, which can determine the label between parent and child.
 * The label is the cardinality.
 * 
 * @author vincent
 *
 * @param <T>
 */
public class EntityTree extends TreeNode<Entity,EntityTree> implements TreeOverlap<EntityTree>, Cost{

	private final List<Query> queries;
	
	public EntityTree(Entity data) {
		this(data, new ArrayList<Query>());
	}
	
	public EntityTree(Entity data, List<Query> queries) {
		super(data);
		
		this.queries = queries;
	}
	
	/**
	 * Creates a new EntityTree from a given linked list of queries.
	 * 
	 * @param queries
	 * @return
	 */
	public static EntityTree createEntityTree(LinkedList<Query> queries) {
		EntityTree prev = null;
		EntityTree tree = null;
		
		for(Query query: queries) {
			EntityTree newTree = new EntityTree(query.getEntity());
			newTree.addQuery(query);
			
			if(prev!=null)
				prev.addChild(newTree);
			else
				tree = newTree;
			
			prev = newTree;
		}
		
		return tree;
	}
	
	/**
	 * Returns the label between child and parent, as the cardinality.
	 * Which is the frequency the parent has the child.
	 * 
	 * @return
	 */
	public int getLabel() {
		Relationship relation = getRelationshipToParent();
		
		if(relation != null)
			return relation.getCardinality(getNode());
		else
			return 1;
	}
	
	/**
	 * Returns queries at this node.
	 * 
	 * @return
	 */
	public List<Query> getQueries() {
		return this.queries;
	}
	
	/**
	 * Returns a list of sequences this tree can be applied to.
	 * 
	 * @return
	 */
	public Set<Sequence> getSequences() {
		Set<Sequence> sequences = new HashSet<Sequence>();
			
		for(Query query: this.queries) {
			sequences.addAll(query.getSequences());
		}
		
		return sequences;
	}
	
	/**
	 * Returns the fields this node contains from the Entity (table).
	 * 
	 * @return
	 */
	public Set<Field> getAffectedFields() {
		Set<Field> fields = new HashSet<Field>();
		
		for(Query qry: queries) 
			fields.addAll(qry.getAffectedFields());
		
		return fields;
	}
	
	/**
	 * Returns whether or not this node has a query part of the given sequence.
	 * 
	 * @param sequence
	 * @return
	 */
	public boolean hasQueryOfSequence(Sequence sequence) {
		return getQueryOfSequence(sequence) != null;
	}
	
	/**
	 * Returns query of a given a sequence.
	 * 
	 * TODO: What if there are multiple options?
	 * 
	 * @param query
	 * @return
	 */
	public Query getQueryOfSequence(Sequence sequence) {
		for(Query query: getQueries()) {
			if(query.getSequences().contains(sequence))
				return query;
		}
		
		return null;
	}
	
	/**
	 * Returns which queries present in the sequence can be answered by this Node.
	 * @param sequence
	 * @return
	 */
	public List<Query> getQueriesPossibleForSequence(Sequence sequence) {
		List<Query> queries = new ArrayList<Query>();
		
		for(Query query: sequence.getQueryPath()) {
			if(query.isAnswerableBy(getQueries()))
				queries.add(query);
		}
		
		return queries;
	}
	
	
	/**
	 * Checks if this EntityTree can answer a given query.
	 * 
	 * @param query
	 * @return
	 */
	public boolean canQuery(Query query) {
		if(query == null)
			return false;
		
		return query.isAnswerableBy(getQueries());
	}
	
	/**
	 * Checks if this contains an explicit given query.
	 * 
	 * @param sequence
	 * @return
	 */
	public boolean hasQuery(Query query) {
		return this.queries.contains(query);
	}
	
	/**
	 * Checks if this EntityTree can answer a given query path.
	 * 
	 * Returns the relevant nodes.
	 * 
	 * TODO: Check final end node; return that one too.
	 */
	public LinkedList<EntityTree> getQueryPaths(LinkedList<Query> path, boolean connectedPath) {
		LinkedList<EntityTree> nodes = new LinkedList<EntityTree>();
		LinkedList<EntityTree> stack = new LinkedList<EntityTree>();
		stack.push(this);
		
		while(!stack.isEmpty()) {
			EntityTree next = stack.pop();
			
			if(next.isAlwaysContainedByParent()) {
				if( next.hasQueryPathHere(path, 0, connectedPath)) 
					nodes.add(next);
				else
					stack.addAll(next.getChildren());
			} else
				stack.addAll(next.getChildren());
		}
		
		return nodes;
	}
	
	/**
	 * Checks if this EntityTree can answer a given query path.
	 * The first query has to be contained in this EntityTree, the second query at one of the children.
	 * 
	 * We start recursively. 
	 * If this tree does not have the path, we ask each child recursively.
	 * 
	 * @param path
	 * @return
	 */
	public boolean hasQueryPath(LinkedList<Query> path, boolean connectedPath) {
		return !getQueryPaths(path, connectedPath).isEmpty();
	}
	
	/**
	 * Checks if this node is an endNode in answering a query starting from the top parent.
	 * 
	 * We simply make a new EntityTree with a single path and apply topParent.hasQueryPathHere
	 * 
	 * @param path
	 * @return
	 */
	public boolean hasQueryPathToParent(LinkedList<Query> path) {
		EntityTree pathTree = getTreePathToThisNode();
		
		return pathTree.getTopParent().hasQueryPathHere(path, 0, false);
	}
	
	/**
	 * The first query has to be contained in this EntityTree, the second query at one of the children.
	 * 
	 * @param path
	 * @return
	 */
	private boolean hasQueryPathHere(LinkedList<Query> path, int index, boolean connectedPath) {
		List<EntityTree> endNodes = getEndNodesOfQueryPath(path, 0, connectedPath);
		
		if(endNodes != null && !endNodes.isEmpty())
			return true;
		else
			return false;
	}
	
	public List<EntityTree> getEndNodesOfQueryPath(LinkedList<Query> path, int index, boolean directConnection) { 
		return getEndNodesOfQueryPath(path, index, directConnection, false);
	}
	
	/**
	 * TRAVERSES THE ENTITYNODE AND SELECTS THE NEXT CHILD WHICH HAS THE NEXT QUERY
	 * IN THE GIVEN LIST.
	 * 
	 * THE FINAL RESULT IS A NODE WHICH CONTAINS THE LAST QUERY IN THE GIVEN PATH,
	 * OR NULL
	 * 
	 * TODO: Returns first end node found.
	 * 
	 * @return
	 */
	private List<EntityTree> getEndNodesOfQueryPath(LinkedList<Query> path, int index, boolean directConnection, boolean skippedNode) {
		List<EntityTree> endNodes = new ArrayList<EntityTree>();
		
		if(path == null || path.isEmpty()) {
			endNodes.add(this);
			return endNodes;
		}
		
		// End-condition
		if(directConnection && index == path.size() - 1 && !canQuery(path.get(index)))
			return null;
		if(index == (path.size() - 1) && canQuery(path.get(index)) )
			if(!skippedNode || isAlwaysContainedByParent()) {
				endNodes.add(this);
				return endNodes;
			}else
				return null;
		
		if(directConnection && !canQuery(path.get(index)))
			return null;
		
		if(!directConnection && !canQuery(path.get(index)) && getChildren().isEmpty())
			return null;
		
		int newIndex = index + 1;
		skippedNode = false;
		
		// Query can be at this node, or at a child further down given the parent rel always exists here.
		if(!directConnection) {
			if(!canQuery(path.get(index)))
				if (isAlwaysContainedByParent()) {
					newIndex = index;
					skippedNode = true;
				}else
					return null;
		}
		
		for(EntityTree child: getChildren()) {			
			List<EntityTree> nodes = child.getEndNodesOfQueryPath(path, newIndex, directConnection, skippedNode);
			
			if(nodes != null && !nodes.isEmpty())
				endNodes.addAll(nodes);
		}
		
		return endNodes;
	}
	
	/**
	 * Returns the first leaf node it finds.
	 * 
	 * @return
	 */
	public EntityTree getRandomLeaf() {
		if(hasChildren())
			return this.getChildren().get(0).getRandomLeaf();
		
		return this;
	}
	
	/**
	 * Query is valid if it affects the entity at this node.
	 * 
	 * @param query
	 * @return
	 */
	public boolean isValidQuery(Query query) {
		return (query.getEntity().equals(getNode()));
	}
	
	/**
	 * Adds the query if it is valid.
	 * 
	 * @param query
	 */
	public void addQuery(Query query) {
		if(!isValidQuery(query))
			throw new IllegalArgumentException("Invalid query which does not affect the entity at this node.");
		
		if(!queries.contains(query))
			queries.add(query);
	}
	
	/**
	 * Adds the query if it is valid.
	 * 
	 * @param query
	 */
	public void addQueries(List<Query> queries) {
		for(Query query: queries)
			this.addQuery(query);
	}
	
	/**
	 * Checks if any child items match a parent.
	 * 
	 * @return
	 */
	public boolean isCyclic() {
		for(EntityTree elem: getElements()) {
			if(elem!= this)
				if(elem.equalsNode(this))
					return true;
		}
		return false;
	}
	
	/**
	 * Checks for each cycle whether it is a legit occurence.
	 * One query must at least belong to sequence which has a similar number of occurences. (>=)
	 * 
	 * @return
	 */
	public boolean isValidCyclic() {
		HashMap<EntityTree, LinkedList<EntityTree>> occurences = getNodesLinkOccurences();
		
		// What-to-check?
		// 
		// for each EntityTree has a nr of occurences:
		//     for each Query:
		//         for each Sequence: not exceed this number.
//		System.out.println(occurences);
		for(EntityTree elem: occurences.keySet()) {
			for(Query query: elem.getQueries()) {
				if(query.occursMaxNrOfTimes() < occurences.get(elem).size()) {
					return false;
				}
			}
		}
		
		return true;
	}
	
	/**
	 * Returns a list of all parent nodes which are repeated at a lower level.
	 * 
	 * @return
	 */
	public List<EntityTree> getCyclicParents() {
		List<EntityTree> cyclicParents = new ArrayList<EntityTree>();
		
		if(isCyclic())
			cyclicParents.add(this);
		
		if(!hasChildren())
			return cyclicParents;
		else{
			for(EntityTree child: getChildren())
				cyclicParents.addAll(child.getCyclicParents());
		}
		
		return cyclicParents;
	}
	
	/**
	 * Returns a map of each node, and the longest link of re-occuring elements.
	 * Multiple options may exist per node, but only one is returned.
	 * 
	 * This is used to check if a node X is repeated N times, this is at least a valid amount.
	 * Each query's sequence is consulted for the node X and its occurrence in isValidCycle.
	 * 
	 * TODO: Cacheable? 
	 * TODO: Saved by top tree node?
	 * 
	 * @return
	 */
	public HashMap<EntityTree, LinkedList<EntityTree>> getNodesLinkOccurences() {
		HashMap<EntityTree, LinkedList<EntityTree>> nodesOccurences = new HashMap<EntityTree, LinkedList<EntityTree>>();
		
		// Loop every elem, check if it already exists.
		// If not, we check the node's occurrences starting from this node.
		for(EntityTree elem: getElements()) {
			EntityTree workingNode = elem;
			
			// Already processed this node?
			for(EntityTree node: nodesOccurences.keySet()) {
				if(node.equalsNode(elem)) {
					workingNode = node;
					break;
				}
			}
			
			if(!nodesOccurences.containsKey(workingNode)) {
				nodesOccurences.put(workingNode, getNodeLinkOccurences(workingNode));
			}
		}
		
		return nodesOccurences;
	}
	
	/**
	 * Returns the longest chain of occurrences for a given node.
	 * 
	 * @param node
	 * @return
	 */
	private LinkedList<EntityTree> getNodeLinkOccurences(EntityTree node) {
		LinkedList<EntityTree> nodeOccurences = new LinkedList<EntityTree>();
		
		if(this.equalsNode(node))
			nodeOccurences.add(this);
		
		LinkedList<EntityTree> longestChild = new LinkedList<EntityTree>();
		
		for(EntityTree child: getChildren()) {
			LinkedList<EntityTree> childOccurences = child.getNodeLinkOccurences(node);
			
			if(childOccurences.size() > longestChild.size())
				longestChild = childOccurences;
		}
		
		nodeOccurences.addAll(longestChild);
		
		return nodeOccurences;
	}
	
	/**
	 * Returns a map of node, and the list of re-occuring elements.
	 * 
	 * @return
	 */
	public HashMap<EntityTree, ArrayList<EntityTree>> getNodesOccurences() {
		HashMap<EntityTree, ArrayList<EntityTree>> nodesOccurences = new HashMap<EntityTree, ArrayList<EntityTree>>();
		
		// Loop every elem, check if it already exists.
		// If not, we check the node's occurences starting from this node.
		for(EntityTree elem: getElements()) {
			EntityTree workingNode = elem;
			
			// Already processed this node?
			for(EntityTree node: nodesOccurences.keySet()) {
				if(node.equalsNode(elem)) {
					workingNode = node;
					break;
				}
			}
			
			if(!nodesOccurences.containsKey(workingNode)) {
				ArrayList<EntityTree> elems = new ArrayList<EntityTree>();
				elems.add(elem);
				nodesOccurences.put(workingNode, elems);
			} else{
				nodesOccurences.get(workingNode).add(elem);
			}
		}
		
		return nodesOccurences;
	}
	
	/**
	 * Travels the tree, and returns the first node which matches the given node.
	 * 
	 * Depth-first.
	 * 
	 * @param tree
	 * @return
	 */
	public EntityTree getFirstChildOccurrence(EntityTree node) {
		if(this.equalsNode(node))
			return this;
		
		for(EntityTree child: getChildren()) {
			EntityTree occurrence = child.getFirstChildOccurrence(node);
			
			if(occurrence != null)
				return occurrence;
		}
		
		return null;
	}
	
	/**
	 * Searches the tree for the first occurrence of a Query in an EntityTree.
	 * 
	 * Breadth-first.
	 * 
	 * @param query
	 * @return
	 */
	public EntityTree getFirstQueryOccurrence(Query query) {
		LinkedList<EntityTree> stack = new LinkedList<EntityTree>();
		
		stack.add(this);
		
		while(!stack.isEmpty()) {
			EntityTree next = stack.pop();
			
			if(next.canQuery(query))
				return next;
			
			stack.addAll(next.getChildren());
		}
		
		return null;
	}
	
	/**
	 * Returns a new EntityTree consisting of a single path down to this node.
	 * 
	 * @return
	 */
	public EntityTree getTreePathToThisNode() {
		EntityTree newTree = new EntityTree(getNode());
		newTree.addQueries(getQueries());
		
		EntityTree nextNode = this.getParent();
		EntityTree prevNode = newTree;
		
		while(nextNode != null) {
			newTree = new EntityTree(nextNode.getNode());
			newTree.addQueries(nextNode.getQueries());
			
			newTree.addChild(prevNode);
			prevNode = newTree;
			
			nextNode = nextNode.getParent();
		}
		
		return newTree;
	}
	
	/**
	 * Calculates the distance between a given child node, and this node.
	 * 
	 * @param child
	 * @return
	 */
	public int getDistance(EntityTree child) {
		
		if(child == this)
			return 0;
		
		int distance = 0;
		
		while(child.hasParent()) {
			child = child.getParent();
			
			if(child.equalsNode(this))
				return distance+1;
			else
				distance++;
		}
		
		return -1;
	}
	
	/**
	 * Does this EntityTree have re-occuring elements.
	 * E.g. Users108 twice? and are they both always contained?
	 * 
	 * E.g. sub-child User108 not always contained.
	 * Parent user108 exist, always contained. => True
	 * 
	 * Items-875 both occur twice always contained => True
	 * Items-875 both occur twice, not always contained => False
	 * 
	 * One sub-child is always contained, other childs become unrelevant.
	 * 
	 * TODO: Is this a correct reasoning?
	 * TODO: Re-occurring can be allowed if the relationship is different. 
	 * 			[Users|Bids|Items|User]. Last users are sellers of items, first users make bids.
	 * 
	 * @return
	 */
	public boolean hasReoccurringElements() {
		HashMap<EntityTree, ArrayList<EntityTree>> nodesOccurences = getNodesOccurences();
		
		// For every node, if size > 1, and one of the nodes is always contained
		// There are re-occurring (or redundant elements).
		for(EntityTree node: nodesOccurences.keySet()) {
			if(nodesOccurences.get(node).size() <= 1)
				continue;
			
			boolean allNotContained = true;
			
			for(EntityTree elem: nodesOccurences.get(node)) {
				if(elem.isAlwaysContainedByParent()) {
					allNotContained = false;
					break;
				}
			}
			
			if(!allNotContained && nodesOccurences.get(node).size() > 1)
				return true;
		}
		
		return false;
	}
	
	/**
	 * The child requires a relationship between the parent, if there is
	 * a parent. 
	 * 
	 * TODO
	 * 
	 * @param child
	 * @return
	 */
	public boolean isValidChild(Entity child) {
		return child != null && child.hasRelationshipWith(getNode());
	}
	
	public boolean isValidChild(EntityTree child) {
		return isValidTree(child) && isValidChild(child.getNode());
	}
	
	public boolean isValidTree(EntityTree tree) {
		return tree != null;
	}
	
	/**
	 * Verify that the child has a relationship with the parent.
	 * Throws an error, if the child has not valid relationship.
	 */
	public EntityTree addChild(Entity child) {
		if(!isValidChild(child))
			return this;
		
		EntityTree childNode = new EntityTree(child);
		childNode.setParent(this);
		this.getChildren().add(childNode);
		this.registerChildForSearch(childNode);
		
		return childNode;
	}
	
	/**
	 * We add the EntityTree as a child.
	 * We register the child in this node's index.
	 * We register all the child's elements in this node's index.
	 * -> This resonates to the top.
	 */
	public EntityTree addChild(EntityTree child) {
		if(!isValidChild(child))
			return this;
		
		child.setParent(this);
		
		this.getChildren().add(child);
		this.registerChildForSearch(child);
		
		for(EntityTree childElem: child.getElements()) {
			if(!childElem.equals(child))
				this.registerChildForSearch(childElem);
		}
		
		return child;
	}
	
	/**
	 * @return Returns the relationship between parent and child if there is a parent,
	 * otherwise null.
	 * 
	 * @exception IllegalStateException
	 * Throws an exception when the two entities parent and child have no relationship.
	 */
	public Relationship getRelationshipToParent() throws IllegalStateException {
		if(this.getParent() == null)
			return null;
		if(!this.getNode().hasRelationshipWith(getParent().getNode()))
			throw new IllegalStateException("Entities in Tree do not have any relationship.");
		else
			return this.getNode().getRelationshipWith(getParent().getNode());
	}
	
	/**
	 * Sum of all node frequencies (=total documents embedded)
	 * E.g. parent =1 + child=5, + childTwo=10, etc.
	 * 
	 * @return
	 */
	public int getNodesTotalFrequency() {
		int frequency = 0;
		
		for(EntityTree tree: getElements()) {
			frequency += tree.getNodeFrequency();
		}
		
		return frequency;
	}
	
	/**
	 * Returns the amount of nodes that will exist of this node in the entire parent record.
	 * 
	 * This based on traversal of the relationship from node to top parent node, and the cardinality.
	 * 
	 * @return
	 */
	public int getNodeFrequency() {
		int frequency;;
		
		if(!hasParent())
			frequency = 1;
		else
			frequency = getParent().getNodeFrequency() * getRelationshipToParent().getCardinalityEntityTargeting(getParent().getNode());
		
		if(frequency==0) {
			System.out.println(getRelationshipToParent().getCardinality().getSourceFreq());
			System.out.println(getRelationshipToParent().getCardinality().getTargetFreq());
			System.out.println("Here");
		}
		
		return frequency;
	}
	
	/**
	 * Special case to see how many times the child would embed the parent.
	 * 
	 * @return
	 */
	public int getNodeFrequencyReverse() {
		if(!hasParent())
			return 1;
		else
			return getParent().getNodeFrequencyReverse() * getRelationshipToParent().getCardinalityEntityTargeting(getNode());
	}
	
	public int getNodeFrequencyWithParent() {
		if(!hasParent())
			return 1;
		else
			return getRelationshipToParent().getCardinalityEntityTargeting(getParent().getNode());
	}
	
	@Override
	public EntityTree clone() {
		EntityTree tree = new EntityTree(getNode(), queries);
		
		for(EntityTree child: getChildren()) {
			tree.addChild(child.clone());
		}
		
		return tree;
	}
	
	/**
	 * Merging two EntityTrees
	 * 
	 * TODO: Cases like: the merge may be (A)
	 * Items -> Users -> Bids (A)
	 * Items -> Bids
	 * 
	 * What if:
	 * Items -> Users -> Bids and
	 * Users -> Bids -> Items 
	 * 
	 * What if: multiple paths(!) children.
	 * Items -> [[Users -> Bids], Bids]
	 * Users -> Bids -> Items
	 * 
	 * What if: multiple paths (complex case)
	 * Items -> [[Users -> Bids], Bids] and
	 * X -> [IceCream->[Items->Bids->Users], [Users -> [Bids->Items],[Regions]]
	 * 
	 * Check what can be merged?
	 * This can create disjoint tops(!!!!)
	 */
	public void complexMerge() {
		// interesting case?
	}
	
	public boolean canMerge(EntityTree tree) {		
		return this.canMergeInto(tree) || tree.canMergeInto(this);
	}
	
	/**
	 * Evaluates whether the two EntityTrees can be merged together.
	 * 
	 * We can merge if the smaller tree overlap with the BigTree.
	 * We can merge if there is no overlap, and the trees are connected.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean canMergeInto(EntityTree tree) {
		int thisSize = this.size();
		int treeSize = tree.getElements().size();
		
		List<EntityTreeOverlap> overlaps;
		
		if(thisSize > treeSize)
			return false;
		
		// Smaller tree searches for its overlaps in the big tree.
		overlaps = overlapsIn(tree);
		
		if(overlaps != null && overlaps.size() >= 1)
			return true;
		
		// When trees are the same size, we also check for a swap around.
		// Example: [Users->Bids] and [Bids->Items], Users does not start in the
		// second tree. 
		if (thisSize == treeSize) {
			overlaps = tree.overlapsIn(this);
			
			if(overlaps != null && overlaps.size() >= 1)
				return true;
		}
		
		// Otherwise check if any nodes are connected...
		return connectedTo(tree);
	}
	
	/**
	 * We assume that this, and the given tree have already been cloned.
	 * 
	 * If there is an overlap, we do a merge overlap.
	 * If they are connected, we merge at the first connection.
	 * 
	 * @param otherTree
	 */
	public void mergeInto(EntityTree otherTree) {
		if(!isValidTree(otherTree))
			return;
		
		int thisSize = this.size();
		int treeSize = otherTree.getElements().size();
		
		List<EntityTreeOverlap> overlaps;
		boolean swapped = false;
		
		// When both trees are equal size, we have to evaluate both ways to merge.
		if(thisSize == treeSize) {
			overlaps = this.overlapsIn(otherTree);
			
			if(overlaps == null || overlaps.size() == 0) {
				swapped = true;
				overlaps = otherTree.overlapsIn(this);
			}
		}
		// We let the smaller tree merge into the other tree.
		else {
			if(thisSize > treeSize) {
				otherTree.mergeInto(this);
				return;
			}
			
			overlaps = this.overlapsIn(otherTree);			
		}
		
		// A: If there is an overlap, we mergeOverlap.
		// Overlap is in the given Tree (unless swapped)
		// at this overlap location, we can start inserting elements from THIS tree.
		if(overlaps != null && overlaps.size() > 0) {
			if(!swapped)
				mergeOverlap(EntityTreeOverlap.retrieveLargestOverlap(overlaps).getPartition());
			else
				otherTree.mergeOverlap(EntityTreeOverlap.retrieveLargestOverlap(overlaps).getPartition());
			
			return;
		}
		
		// B: No overlap? => We check if both trees are connected
		mergeConnection(otherTree);	
	}
	
	/**
	 * Merge the other tree at a given leaf node.
	 * 
	 * @param leafNode
	 * @param otherTree
	 * @return
	 */
	private EntityTree mergeConnection(EntityTree otherTree) {
		if(!isValidTree(otherTree))
			return this;
		
		EntityTree connectionPoint = null;
		
		connectionPoint = this.connectionAtTopOf(otherTree);
		
		if(connectionPoint != null) {
			connectionPoint.addChild(otherTree);
		} else {
			connectionPoint = this.connectionAtBottomOf(otherTree);
			
			if(connectionPoint != null) {
				connectionPoint.addChild(this);
			} else {
				throw new IllegalArgumentException("Tried to merge two trees which can not be merged.");
			}
		}	
		
		return connectionPoint.getTopParent();
	}
	
	/**
	 * Merges this tree at the partition location of the given tree.
	 * We assume the given tree is already cloned.
	 * 
	 * @param partition	Partition in a foreign tree.
	 * 		  We assume this tree starts at the same foreign partition tree node.
	 * @return
	 */
	private void mergeOverlap(EntityTree partition) {
		if(!partition.equalsNode(this))
			throw new IllegalArgumentException("No similar starting point for MergeOverlap");
		if(!this.overlapBetween(partition)) 
			throw new IllegalArgumentException("There is no overlap between this tree, and the given tree.");
		
		mergeOverlapRecursively(partition);
	}
	
	private void mergeOverlapRecursively(EntityTree partition) {
		if(partition.compareTo(this) != 0)
			throw new IllegalArgumentException("Partition should start with the top of this this tree.");
		
		// Loop over our children:
		// 1) Child matches Partition Child: Our child recursively merges with the partition's child.
		// 2) No match with Partition Child: then we simply add the this new child.
		for(EntityTree child: getChildren()) {
			if(partition.getChildren().contains(child)) {
				int partitionChildIndex = partition.getChildren().indexOf(child);
				
				child.mergeOverlapRecursively(partition.getChildren().get(partitionChildIndex));	
			} else {
				partition.addChild(child);
			}
		}
	}
	
	/**
	 * Checks if there exists any overlap between the two trees.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean overlapBetween(EntityTree tree) {
		if(!isValidTree(tree))
			return false;
		
		List<EntityTreeOverlap> overlap = null;
		
		if(tree.size() > this.size()) 
			overlap = this.overlapsIn(tree);
		else
			overlap = tree.overlapsIn(this);
		
		return overlap != null && overlap.size() >= 1;
	}
	
	/**
	 * Checks if given tree matches the top *completely*.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean overlapsAtTopOf(EntityTree tree) {
		if(!isValidTree(tree))
			return false;
		
		if(getTopParent().compareTo(tree.getTopParent()) != 0)
			return false;
		
		EntityTreeOverlap overlap = overlapLargestIn(tree);
		
		return overlap.getSize() == this.size();
	}
	
	/**
	 * Checks if given tree matches the bottom *completely*.
	 * 
	 * @param tree
	 * @return
	 */
	public boolean overlapsAtBottomOf(EntityTree tree) {
		EntityTree overlap = getSubTreeOf(tree);
		
		if(overlap == null) 
			return false;
		
		List<EntityTree> overlapLeafs = overlap.getAllLeafNodes();
		List<EntityTree> theseLeafs = getAllLeafNodes();
		
		return theseLeafs.containsAll(overlapLeafs) 
				&& isSubTreeOf(tree);
	}
	
	/**
	 * Returns the partition of the given tree, where this tree
	 * has the largest match.
	 * 
	 * @param tree
	 * @return
	 */
	public EntityTreeOverlap overlapLargestIn(EntityTree tree) {
		EntityTreeOverlap bestOverlap = null;
		List<EntityTreeOverlap> overlaps = overlapsIn(tree);
		
		if(overlaps == null)
			return null;
		
		for(EntityTreeOverlap overlap: overlaps) {
			if(bestOverlap == null || overlap.getSize() > bestOverlap.getSize())
				bestOverlap = overlap;
		}
		
		return bestOverlap;
	}
	
	/**
	 * Scan the given tree to determine where this tree occurs
	 * and how many of its elements match.
	 * 
	 * The overlapping portions are references to the given tree.
	 * And the corresponding size of the overlap.
	 * 
	 * e.g. itemsUsersTree in bidsUsersItemsTree; overlap at Items (size=1?)
	 * 
	 * @param tree
	 * @return Partitions which refer to elements of tree.
	 */
	public List<EntityTreeOverlap> overlapsIn(EntityTree tree) {
		if(tree == null)
			return null;
		
		if(getElements().size() > tree.getElements().size())
			return null;
		
		List<EntityTreeOverlap> overlapPartitions = new ArrayList<EntityTreeOverlap>();
		
		if(tree==this) {
			EntityTreeOverlap overlap = new EntityTreeOverlap(this.size(), this);
			overlapPartitions.add(overlap);
			
			return overlapPartitions;
		}
		
		/**
		 * Take a node, search where it occurs in this tree.
		 * From that point, identify whether this partition has a path to each child.
		 */
		List<EntityTree> partitions = tree.findAllTreeNodesLike(this);
		
		if(partitions.isEmpty())
			return overlapPartitions;
		
		for(EntityTree partition: partitions) {
			EntityTreeOverlap overlap = overlapLargestPartitionOf(partition);
			
			if(overlap.getSize() > 0) {				
				overlapPartitions.add(overlap);
			}
		}
		
		return overlapPartitions;
	}
	
	/**
	 * Checks if this is tree occurs in sequence in the partition.
	 * 
	 * e.g. itemsUsersTree in bidsUsersItemsTree; overlap at Items (size=1?)
	 * 
	 * @param partition
	 * @return
	 */
	protected EntityTreeOverlap overlapLargestPartitionOf(EntityTree partition) {
		if(partition.compareTo(this) != 0)
			throw new IllegalArgumentException("Partition should start with the top of this this tree.");
		
		if(getChildren().isEmpty())
			return new EntityTreeOverlap(1, partition);
		
		List<EntityTree> remainingChildren = new ArrayList<EntityTree>(getChildren());
		
		int childrenOverlapSize = 0;
		
		// Loop over the partition's children, if it is matches one of ours. 
		// Then check that sub-partition, with our child's sub-partition.
		for(EntityTree partitionChild: partition.getChildren()) {
			if(remainingChildren.contains(partitionChild)) {
				// Find actual index of our child instead of partition's.
				int thisChildIndex = getChildren().indexOf(partitionChild);
				
				EntityTreeOverlap overlapSize = getChildren().get(thisChildIndex).overlapLargestPartitionOf(partitionChild);
				
				if(overlapSize.getSize() >= 1) {
//						remainingChildren.remove(partitionChild);
						childrenOverlapSize += overlapSize.getSize();
				}
			}
		}
		
		return new EntityTreeOverlap(childrenOverlapSize+1, partition);
	}
	
	@Override
	public boolean isSubTreeOf(EntityTree tree) {
		if(getSubTreeOf(tree) != null)
			return true;
		else
			return false;
	}
	
	
	/**
	 * This functionality checks explicitly if this entire graph is exactly contained.
	 * 
	 * Start at the parent of given tree, traverse all nodes and check
	 * if they match the paths available in this tree.
	 * 
	 * The elements have to be connected in the right order.
	 * 
	 * For example: Bids -> Items -> Users (Y)
	 * Users(Y) is not a SubTree. Since some users may not have any bids.
	 * 
	 * Returns the portion of the given tree, at which this tree matches.
	 * 
	 * @param tree
	 * @return
	 */
	public EntityTree getSubTreeOf(EntityTree tree) {
		if(tree == null)
			return null;
		
		if(tree==this)
			return tree;
		
		if(getElements().size() > tree.getElements().size())
			return null;
		
		/**
		 * Take a node, search where it occurs in this tree.
		 * From that point, identify whether this partition has a path to each child.
		 */
		List<EntityTree> partitions = tree.findAllTreeNodesLike(this);
		
		if(partitions.isEmpty())
			return null;
		
		for(EntityTree partition: partitions) {
			if(isSubTreeOfPartition(partition))
				return partition;
		}
		
//	    PHASE 2: Unlinked structures e.g. search A -> B and A -> C -> B exists.
//		for(EntityTree partition: tree.findAllTreeNodes(tree)) {
//			LinkedList<EntityTree> seenNodes = new LinkedList<EntityTree>();
//			seenNodes.add(tree);
//			
//			if(isSubTreeOfPartition(partition, tree, seenNodes))
//				return true;
//		}
		
		return null;
	}
	
	/**
	 * Checks if this is tree occurs in sequence in the partition.
	 * 
	 * E.g. this: A->B->C
	 * [A|D,B|C] = True
	 * [A|D|...] = False
	 * 
	 * E.g.: A->[B->D|C->F]
	 * [A->[B|C->F] = False
	 * [A->[X|B->[D,G]|C->F]] = True
	 * 
	 * @param partition
	 * @return
	 */
	private boolean isSubTreeOfPartition(EntityTree partition) {
		EntityTreeOverlap overlap = overlapLargestIn(partition);
		
		return overlap != null && this.size() == overlap.getSize();
	}
	
	/**
	 * Checks if the this tree is a subset of the given tree.
	 * 
	 * For example: Bids -> Items -> Users (Y)
	 * Users(Y) is not a SubTree. Since some users may not have any bids.
	 * 
	 * SOLUTION in: isSubTreeOf() !!
	 * 
	 * @param tree
	 * @return
	 */
	public boolean isSubSetOf(EntityTree tree) {
		// has to be a subTree to begin with.
		// then from this node to the top, the relationships have to always exist(!).
		// Items -> Users, Users is a subSetOf [Items,Users] if Each user always has 1 item.
		
		EntityTree subTree = getSubTreeOf(tree);
		
		if(subTree == null)
			return false;
		
		// Traverse to the top of subTree. Making sure it is fully contained.
		return subTree.isAlwaysContainedByParent();
	}
	
	/**
	 * Traverses to the top and checks if the relationship between
	 * this node and the parent is ever present.
	 * 
	 * Items -> Users: True  for Users when Users:Items is 1..N
	 * Items -> Users: False for Users when Users:Items is 0..N
	 * 
	 * Means the parent collection fully represents the child's collection due though at least 1-1.
	 * 
	 */
	public boolean isAlwaysContainedByParent() {
		if(!hasParent())
			return true;
		
		if(getParent().getNode().hasRelationshipWith(getNode())) {
			if(!getParent().getNode().getRelationshipWith(getNode()).alwaysExistsAtSource())
				return false;
			else
				return getParent().isAlwaysContainedByParent();
		} else
			return false;
			
	}
	
//	/**
//	 * Checks if the tree overlaps with another tree.
//	 * 
//	 * @param tree
//	 * @return
//	 */
//	@Deprecated
//	public List<EntityTree> overlap(EntityTree tree) {
//		Iterator<EntityTree> iterator = tree.getTopParent().iterator();
//		List<EntityTree> overlap = new ArrayList<EntityTree>();
//		
//		while(iterator.hasNext()) {
//			EntityTree node = iterator.next();
//			EntityTree search = tree.findTreeNode(node);
//			
//			if(search != null) {
//				overlap.add(search);
//			}
//		}
//		
//		return overlap;
//	}
//	
//	/**
//	 * Returns the portion of connected nodes which overlap.
//	 * 
//	 * Start at the top of the given tree. Iterate towards the bottom,
//	 * until a matching part is found. Then add the connected nodes which also
//	 * match this tree.
//	 * 
//	 * @param tree
//	 * @return
//	 */
//	@Deprecated
//	public EntityTree overlapTree(EntityTree tree) {
//		List<EntityTree> overlap = overlap(tree);
//		
//		Iterator<EntityTree> listIterator = overlap.iterator();
//		
//		while(listIterator.hasNext()) {
//			EntityTree node = listIterator.next();
//			
//			if(!overlap.contains(node.getParent()) ){
//				return node;
//			}
//		}
//		
//		return null;
//	}
	
	/**
	 * There has to exist at least one relationship between any of these leafs entity nodes,
	 * and the tree's top entity node.
	 * 
	 * However, an EntityTree node is determined by its queries.
	 * WE should only merge that which makes sense.
	 * 
	 * E.g. sequence: A -> B -> C -> D.
	 * 
	 */
	public EntityTree connectionAtTopOf(EntityTree tree) {
		if(!isValidTree(tree))
			return null;
		
		if(overlapBetween(tree))
			return null;
		
		// Leaf - Top have to be in the same sequence, and same order!
		// e.g. [A,B] and [C,D].		
		for(EntityTree leaf: getAllLeafNodes()) {
			for(Query query: leaf.getQueries()) {
				for(Query queryTop: tree.getQueries()) {
					// 1) Find matching query per leaf node
					// 2) Make sure sequence is split at this point.
					List<Sequence> overlap = Sequence.overlapBetween(query.getSequences(), queryTop.getSequences());
					
					for(Sequence seq: overlap) {
						// 2: Check of gedeelte overeenkomt met het deel van de gehele Sequence.
						// TODO: enkel A->C indien [A->C] of ook [A->B->C]
						if(seq.isSequenceConnectedBetween(leaf, tree))
							return leaf;
					}
				}
			}
		}
		
		return null;
	}
	
	public EntityTree connectionAtBottomOf(EntityTree tree) {
		return tree.connectionAtTopOf(this);
	}
	
	/**
	 * There has to exist at least one relationship between any of these leafs entity nodes,
	 * and the tree's top entity node.
	 * 
	 */
	@Override
	public boolean connectedAtTopOf(EntityTree tree) {
		if(connectionAtTopOf(tree) != null)
			return true;
		else
			return false;
	}

	@Override
	public boolean connectedAtBottomOf(EntityTree tree) {
		if(!isValidTree(tree))
			return false;
		
		return tree.connectedAtTopOf(this);
	}

	@Override
	public boolean connectedTo(EntityTree otherCandidate) {
		if(!isValidTree(otherCandidate))
			return false;
		
		return connectedAtBottomOf(otherCandidate) || connectedAtTopOf(otherCandidate);
	}
	
	/**
	 * Checks if the parent nodes are similar.
	 * 
	 * @param o
	 * @return
	 */
	public boolean equalsNode(EntityTree o) {
		return o.getNode().equals(getNode()) && o.getQueries().equals(getQueries());
	}
	
	/**
	 * Checks if the same top node exists in this tree.
	 * 
	 * @param node
	 * @return
	 */
	public boolean containsChildNode(EntityTree node) {
		for(EntityTree child: getChildren())
			if(child.equalsNode(node))
				return true;
		
		return false;
	}
	
	/**
	 * Takes a node, searches for the similar node in this tree and returns it.
	 * 
	 * @param node
	 * @return
	 */
	public EntityTree getChildNode(EntityTree node) {
		for(EntityTree child: getChildren())
			if(child.equalsNode(node))
				return child;
		
		return null;
	}
	
	@Override
	public int compareTo(EntityTree o) {
		if(queries.size() > o.getQueries().size())
			return 1;
		else if(queries.size() < o.getQueries().size())
			return -1;
		
		if(equalsNode(o))
			return 0;
		else
			return -1;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
//		result = prime * result + getNode().hashCode();
		result = prime * result + ((queries == null) ? 0 : queries.hashCode());
		
		for(EntityTree child: getChildren())
			result = prime * result + child.hashCode();
		
		return result;
	}

	/**
	 * Does not check if the children are equal.
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		EntityTree other = (EntityTree) obj;
		if (queries == null) {
			if (other.queries != null)
				return false;
		} else if (!queries.equals(other.getQueries()))
			return false;
		return true;
	}
	
	public boolean equalsEntireTree(EntityTree tree) {
		if(!this.equals(tree))
			return false;
		
		if(!getChildren().equals(tree.getChildren()))
			return false;
		
		for(EntityTree child: tree.getChildren()) {
			if(!getChildNode(child).equalsEntireTree(child))
				return false;
		}
		
		return true;
	}

	@Override
	public String toString() {
		String tree ="[ " + getNode().getName() + getQueries().hashCode() % 1000;
		
		for(EntityTree child: getChildren()) {
			tree += " " +  child.toString();
		}
		
		tree += " ]";
		
		return tree;
	}

	@Override
	public int getCost() {
		return new CostCalculatorEntityTree(this).getCost();
	}

}
