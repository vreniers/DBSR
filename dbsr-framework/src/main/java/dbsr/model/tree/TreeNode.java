package dbsr.model.tree;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Add label about cardinality (one-to-one, etc. / relationship).
 * Can ask label through Parent - Entity relation.
 * 
 * @author vincent
 *
 * @param <T>
 */
abstract public class TreeNode<T, K extends TreeNode<T,K>> implements Iterable<K>, Comparable<K> {

	private T data;
	private K parent;
	private List<K> children;

	public TreeNode(T data) {
		this.data = data;
		this.children = new LinkedList<K>();
		this.elementsIndex = new LinkedList<K>();
		this.elementsIndex.add((K) this);
	}
	
	public T getNode() {
		return this.data;
	}
	
	public K getParent() {
		return this.parent;
	}
	
	public K getTopParent() {
		if(getParent() != null)
			return this.getParent().getTopParent();
		else
			return (K) this;
	}
	
	/**
	 * Scans the entire tree to determine the size of this entityTree.
	 * 
	 * @return
	 */
	public int size() {
		int size = 1;
		
		for(K child: getChildren()) 
			size += child.size();
		
		return size;
	}
	
	/**
	 * Returns all the nodes without any children.
	 * 
	 * @return
	 */
	public List<K> getAllLeafNodes() {
		if(getParent() != null)
			return getTopParent().getAllLeafNodes();
		
		return getChildLeafs();
	}
	
	public List<K> getChildLeafs() {
		ArrayList<K> leafNodes = new ArrayList<K>();
		
		for(K child: children) {
			if(!child.hasChildren())
				leafNodes.add(child);
			else
				leafNodes.addAll(child.getChildLeafs());
		}
		
		if(leafNodes.isEmpty())
			leafNodes.add((K) this);
		
		return leafNodes;
	}
	
	/**
	 * Checks whether this Tree is the parent of the given Tree.
	 * 
	 * @param node
	 * @return
	 */
	public boolean isParentOf(K node) {
		if(node == null)
			return false;
		
		if(node.hasParent() && node.getParent() == this)
			return true;
		else{
			if(!node.hasParent())
				return false;
			
			return isParentOf(node.getParent());
		}
	}
	
	public boolean hasParent() {
		return this.parent != null;
	}
	
	public void setParent(K parent) {
		this.parent = parent;
	}
	
	public List<K> getChildren() {
		return this.children;
	}
	
	public boolean hasChildren() {
		return !this.children.isEmpty();
	}
	
	public void addChildren(List<K> children) {
		this.children.addAll(children);
		
		for(K child: children)
			child.setParent((K) this);
	}
	
	public boolean isRoot() {
		return parent == null;
	}
	
	/**
	 * Returns the first leaf node it finds.
	 * 
	 * @return
	 */
	public K getRandomLeaf() {
		if(hasChildren())
			return this.getChildren().get(0).getRandomLeaf();
		
		return (K) this;
	}

	public boolean isLeaf() {
		return children.size() == 0;
	}

	private List<K> elementsIndex;
	
	/**
	 * Returns list of entire set of tree nodes.
	 */
	public List<K> getElements() {
		return this.elementsIndex;
	}

	public K addChild(K childNode) {
		childNode.setParent((K) this);
		this.children.add(childNode);
		this.registerChildForSearch(childNode);
		return childNode;
	}

	public int getLevel() {
		if (this.isRoot())
			return 0;
		else
			return parent.getLevel() + 1;
	}
	
	/**
	 * Maximum depth in this tree, starting from this node.
	 */
	public int getMaxDepth() {
		int maxDepth = 0;
		
		for(K child: getChildren()) {
			maxDepth = Math.max(child.getMaxDepth(), maxDepth);
		}
		
		return maxDepth+1;
	}
	
	/**
	 * Largest width at a given point in the tree, starting from this node.
	 */
	public int getMaxWidth() {
		int maxWidth = getChildren().size();
		
		for(K child: getChildren()) {
			maxWidth = Math.max(child.getMaxWidth(), maxWidth);
		}
		
		return maxWidth;
	}

	protected void registerChildForSearch(K node) {
		elementsIndex.add(node);
		if (parent != null)
			parent.registerChildForSearch(node);
	}

	/**
	 * Finds a similar node to the one given.
	 * Does not take children into consideration.
	 * 
	 * @param cmp
	 * @return
	 */
	public K findTreeNode(K cmp) {
		for (K element : this.elementsIndex) {
			if (cmp.compareTo(element) == 0)
				return element;
		}

		return null;
	}
	
	/**
	 * Returns all similar tree nodes to the one given.
	 * 
	 * @param cmp
	 * @return
	 */
	public List<K> findAllTreeNodesLike(K cmp) {
		if(hasParent())
			return getTopParent().findAllTreeNodesLike(cmp);
		
		ArrayList<K> treeNodes = new ArrayList<K>();
		
		for (K element : this.elementsIndex) {
			if (cmp.compareTo(element) == 0)
				treeNodes.add(element);
		}
		
		return treeNodes;
	}

	@Override
	public String toString() {
		return data != null ? data.toString() : "[data null]";
	}

	@Override
	public Iterator<K> iterator() {
		TreeNodeIter<T,K> iter = new TreeNodeIter<T,K>((K) this);
		return iter;
	}

}