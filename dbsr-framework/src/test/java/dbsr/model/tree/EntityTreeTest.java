package dbsr.model.tree;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

/**
 * @author root
 *
 */
public class EntityTreeTest {

	private Sequence usersBidsItemsUser, usersBids, bidsItems, itemsUsers, bidsUsers, usersItems = new Sequence();
	
	private Entity users, items, bids;
	
	// usersBidsItemsUsers with itemsUsers again at 1 level.
	private EntityTree largeTree;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}
	
	/**
	 * Creates different entity trees from sequences.
	 * Users-> Bids -> Items -> users
	 * Users-> Bids
	 * Bids -> Items
	 * Items-> Seller 
	 * and
	 * Bids -> Seller
	 * 
	 * Uses these combinations to check for subsets and overlaps of the corresponding EntityTrees.
	 * 
	 * Item ALWAYS has bids in this test case!
	 * 
	 * @throws Exception
	 */
	@Before
	public void setUp() throws Exception {
		usersBidsItemsUser = new Sequence();
		
		users = createEntity("users");
		items = createEntity("items");
		bids = createEntity("bids");
		
		Relationship usersPlaceBids = new Relationship("usersPlaceBids", users, bids, RelationshipType.OneToMany);
		Relationship bidsAreOnItems = new Relationship("bidsAreOnItems", bids, items, RelationshipType.ManyToOne);
		Relationship usersSellItems = new Relationship("usersSellItems", users, items, RelationshipType.OneToMany);
		//Relationship itemsHaveSeller = new Relationship("itemHasSeller", items, users, RelationshipType.ManyToOne); //EQUI
		
		
		// Set cardinalities
		usersPlaceBids.setCardinality(new Cardinality(1,3));
		bidsAreOnItems.setCardinality(new Cardinality(10,1));
		usersSellItems.setCardinality(new Cardinality(1,5));
		
		Query usersQuery = createQuery(users);
		Query bidsQuery = createQuery(bids);
		Query itemsQuery = createQuery(items);
		Query usersTwoQuery = createQuery(users);
		
		usersBidsItemsUser.addQuery(usersQuery);
		usersBidsItemsUser.addQuery(bidsQuery);
		usersBidsItemsUser.addQuery(itemsQuery);
		usersBidsItemsUser.addQuery(usersTwoQuery);
		
		usersBids = new Sequence();
		usersBids.addQuery(usersQuery);
		usersBids.addQuery(bidsQuery);
		
		bidsItems = new Sequence();
		bidsItems.addQuery(bidsQuery);
		bidsItems.addQuery(itemsQuery);
		
		itemsUsers = new Sequence();
		itemsUsers.addQuery(itemsQuery);
		itemsUsers.addQuery(usersTwoQuery);
		
		bidsUsers = new Sequence();
		bidsUsers.addQuery(bidsQuery);
		bidsUsers.addQuery(usersQuery);
		
		usersItems = new Sequence();
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(itemsQuery);
		
		
		// Make large tree
		largeTree = usersBidsItemsUser.getEntityTree().clone();
		EntityTree itemsUsersTree = itemsUsers.getEntityTree();
		
		largeTree.addChild(itemsUsersTree);		
	}
	
	private static Entity createEntity(String tableName) {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field(tableName, "name");
		Field id = new Field(tableName, "id");
		
		fields.add(name);
		fields.add(id);
		
		Entity users = new Entity(tableName, id, fields);
				
		return users;
	}
	
	private static Query createQuery(Entity entity) {
		Set<Field> conditionalFields = new HashSet<Field>();
		
		Query query = new SelectQuery(entity, new Integer(10), entity.getFields(), conditionalFields);
		
		return query;
	}
	
	private static Query createQuery(Entity entity, Set<Field> conditionalFields) {
		Query query = new SelectQuery(entity, new Integer(10), entity.getFields(), conditionalFields);
		
		return query;
	}
	
	private static Query createQuery(Entity entity, Set<Field> conditionalFields, Set<Field> selectFields) {
		Query query = new SelectQuery(entity, new Integer(10), selectFields, conditionalFields);
		
		return query;
	}
	
	@Test
	public void testGetEntityTree() {
		EntityTree userBids = usersBids.getEntityTree();
//		System.out.println(userBids);
		assertEquals(userBids.getNode(), users);
		assertEquals(userBids.getChildren().get(0).getNode(), bids);
	}
	
	@Test
	public void testGetRandomLeaf() {
		EntityTree userBids = usersBids.getEntityTree();
		assertEquals(userBids.getRandomLeaf().getNode(), bids);
	}
	
	@Test
	public void testCanMerge() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(usersBids.getEntityTree().canMerge(bigTree));
		assertTrue(bidsItems.getEntityTree().canMerge(bigTree));
		assertTrue(itemsUsers.getEntityTree().canMerge(bigTree));
		assertTrue(bidsUsers.getEntityTree().canMerge(bigTree));
		
		assertTrue(usersBids.getEntityTree().canMerge(bidsItems.getEntityTree()));
		assertTrue(usersBids.getEntityTree().canMerge(itemsUsers.getEntityTree()));
	}
	
	/**
	 * Problem case: 
	 * Users1 -> Users2 and User1 -> Bids
	 * 
	 * Results in:
	 * Users1 -> [User2] [Users2]
	 */
	@Test
	public void testMerge() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree usersBids = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		usersBids.addChild(new EntityTree(bigTree.getChildren().get(0).getNode(),bigTree.getChildren().get(0).getQueries()));
		
		EntityTree usersUsers = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		usersUsers.addChild(new EntityTree(bigTree.getRandomLeaf().getNode(), bigTree.getRandomLeaf().getQueries()));
		
		assertTrue(usersBids.canMerge(usersUsers));
		usersBids.mergeInto(usersUsers);
//		System.out.println(usersBids);
//		System.out.println(usersUsers);
	}
	
	/**
	 * TODO: Secondary Index
	 * Original: QueryPlan [candidates=[ users-18 ] -> [ items390 ], indexes=[]]
	 * New:      QueryPlan [candidates=[ bids94 [ users-18 ] ] -> [ bids94 [ items390 [ users270 ] ] ], indexes=[]]
	 * 
	 * This works because each user has at least one bid.
	 * And each item has at least one bid.
	 */
	
	/**
	 * TODO
	 * [candidates=[ users-18 [ bids94 ] ] -> [ bids94 [ items390 ] ]
	 * 
	 * Result:
	 * [ users-18 [ bids94 [ items 390 ] ] ]
	 */
	@Test
	public void testMergeOverlap() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree usersBids = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		usersBids.addChild(new EntityTree(bigTree.getChildren().get(0).getNode(),bigTree.getChildren().get(0).getQueries()));
		
		EntityTree bidsItems = new EntityTree(bigTree.getChildren().get(0).getNode(),bigTree.getChildren().get(0).getQueries());
		bidsItems.addChild(new EntityTree(bigTree.getRandomLeaf().getParent().getNode(), bigTree.getRandomLeaf().getParent().getQueries()));
		
		assertTrue(usersBids.canMerge(bidsItems));
		usersBids.mergeInto(bidsItems);
//		System.out.println(usersBids);
//		System.out.println(bidsItems);
	}
	
	/**
	 * Test overlaps in
	 * 
	 * usersBids.overlapsIn(bidsItems) => None
	 * bidsItems.overlapsIn(usersBids) => at Bids of usersBids
	 */
	@Test
	public void testOverlapsIn() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree usersBids = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		usersBids.addChild(new EntityTree(bigTree.getChildren().get(0).getNode(),bigTree.getChildren().get(0).getQueries()));
		
		EntityTree bidsItems = new EntityTree(bigTree.getChildren().get(0).getNode(),bigTree.getChildren().get(0).getQueries());
		bidsItems.addChild(new EntityTree(bigTree.getRandomLeaf().getParent().getNode(), bigTree.getRandomLeaf().getParent().getQueries()));
		
//		System.out.println(bidsItems);
//		System.out.println(usersBids);
		
//		System.out.println("---overlapIn---");
		assertEquals(usersBids.overlapsIn(bidsItems).size(), 0);
		assertEquals(bidsItems.overlapsIn(usersBids).size(), 1);
		
		List<EntityTreeOverlap> overlap = bidsItems.overlapsIn(usersBids);
		assertEquals(overlap.get(0).getPartition(), bidsItems.overlapLargestIn(usersBids).getPartition());
		assertEquals(overlap.get(0).getSize(), bidsItems.overlapLargestIn(usersBids).getSize());
		
		// The partition should be located inside of usersBids, at bids.
		EntityTreeOverlap overlapResult = bidsItems.overlapLargestIn(usersBids);
		
//		System.out.println(overlapResult.getPartition());
//		System.out.println(overlapResult.getPartition().getTopParent());
		assertTrue(overlapResult.getPartition().equalsEntireTree(usersBids.getRandomLeaf()));
		assertTrue(overlapResult.getPartition().getTopParent().equalsEntireTree(usersBids));
		
	}
	
	/**
	 * TODO make a test which actually checks for the order of path:
	 * 
	 * Bids -> Users should fail in Users -> Bids -> Items -> Users
	 * 
	 * E.g. problematic when some users don't have bids!!
	 */
	@Test
	public void testIsSubsetOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(usersBids.getEntityTree().isSubSetOf(bigTree));
		assertTrue(bidsItems.getEntityTree().isSubSetOf(bigTree));
		assertTrue(itemsUsers.getEntityTree().isSubSetOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().isSubSetOf(bigTree));
		
		// Each user has a bid
		EntityTree users = new EntityTree( bidsUsers.getEntityTree().getChildren().get(0).getNode(), bidsUsers.getEntityTree().getChildren().get(0).getQueries());
		assertTrue(users.isSubSetOf(bidsUsers.getEntityTree()));
		
		// Not every user sells an item
		assertFalse(users.isSubSetOf(itemsUsers.getEntityTree()));
		
		// User is contained in top parent user.
		assertTrue(users.isSubSetOf(bigTree));
	}
	
	@Test
	public void testIsSubTreeOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
//		System.out.println(bigTree);
		assertTrue(usersBids.getEntityTree().isSubTreeOf(bigTree));
		assertTrue(bidsItems.getEntityTree().isSubTreeOf(bigTree));
		assertTrue(itemsUsers.getEntityTree().isSubTreeOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().isSubTreeOf(bigTree));
	}
	
	/**
	 * Test becomes cyclic after merge? 
	 * Cyclic allowed?
	 * 
	 * Get cyclic elements(?)
	 * Parent -> .. -> Child
	 * 
	 * Is this allowed? 
	 */
	@Test
	public void testIsCyclic() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(bigTree.isCyclic());
		assertFalse(bigTree.getChildren().get(0).isCyclic());
	}
	
	/**
	 * Test wheter a structure's occurence, corresponds to a sequence's demands.
	 * E.g. sequences are:
	 * Users1 -> Bids -> Items -> Users1
	 * 
	 * not allowed is Users1 -> Users1 -> Users1
	 */
	@Test
	public void testIsValidCyclic() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(bigTree.isValidCyclic());
		
		EntityTree users = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		bigTree.getChildren().get(0).addChild(users);
		
		assertTrue(bigTree.isValidCyclic());
		
		bigTree.getRandomLeaf().addChild(users);
		
		assertFalse(bigTree.isValidCyclic());
	}
	
	@Test
	public void testIsParentOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(bigTree.isParentOf(bigTree.getChildren().get(0)));
		assertTrue(bigTree.isParentOf(bigTree.getRandomLeaf()));
		assertFalse(bigTree.getChildren().get(0).isParentOf(bigTree));
		assertFalse(bigTree.getRandomLeaf().isParentOf(bigTree));
		
		assertEquals(bigTree.getChildren().get(0).getParent(), bigTree);
		assertEquals(bigTree.getRandomLeaf().getParent(), bigTree.getChildren().get(0).getChildren().get(0));
		assertEquals(bigTree.getChildren().get(0).getChildren().get(0).getParent(), bigTree.getChildren().get(0));
	}
	
	@Test
	public void testgetParentAfterAddChild() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		bigTree.addChild(bidsItems.getEntityTree());
		
		// System.out.println(bigTree.getChildren().get(0));
		// System.out.println(bigTree.getChildren().get(1));
		
		assertEquals(bigTree.getChildren().get(0).getParent(), bigTree);
		assertEquals(bigTree.getChildren().get(0).getChildren().get(0).getParent(), bigTree.getChildren().get(0));
		
		// assertEquals(bigTree.getChildren().get(0).getChildren().get(0).getParent(), bigTree.getChildren().get(0));
		
		// assertEquals(bigTree.getChildren().get(1).getParent(), bigTree);
		// assertEquals(bigTree.getChildren().get(1).getChildren().get(1).getParent(), bigTree.getChildren().get(1));
		
	}
	
	@Test
	public void testGetCyclicParents() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		List<EntityTree> cyclicElems = bigTree.getCyclicParents();
		assertEquals(cyclicElems.size(), 1);
		assertEquals(cyclicElems.get(0), bigTree);
	}
	
	@Test
	public void testGetCyclicParentsMultiple() {
		EntityTree bigTree  = usersBidsItemsUser.getEntityTree();
		bigTree.getChildren().get(0).addChild(usersBids.getEntityTree());
		
		// Users -> Bids -> [Users -> Bids][Items -> Users] 
		List<EntityTree> cyclicElems = bigTree.getCyclicParents();
		
		assertEquals(cyclicElems.size(), 2);
		
		// Users -> [Users->Bids][Bids-> Items -> Users] (=1) (occurs twice)
		bigTree  = usersBidsItemsUser.getEntityTree();
		bigTree.addChild(usersBids.getEntityTree());
		
		cyclicElems = bigTree.getCyclicParents();
		
		assertEquals(cyclicElems.size(), 1);
	}
	
	@Test
	public void testGetNodesOccurences() {
		EntityTree bigTree  = usersBidsItemsUser.getEntityTree();
		bigTree.getChildren().get(0).addChild(usersBids.getEntityTree());
		
		// Users -> Bids -> [Users -> Bids][Items -> Users] 
		HashMap<EntityTree, LinkedList<EntityTree>> cyclicElems = bigTree.getNodesLinkOccurences();
		
		// 3 types of EntityTrees
		assertEquals(cyclicElems.size(), 3);
		
//		for(EntityTree elem: cyclicElems.keySet()) {
//			System.out.println(elem);
//			System.out.println(cyclicElems.get(elem));
//			System.out.println("---");
//		}
	}
	
	@Test
	public void testOverlapsAtTopOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree smallerTree = bigTree.getChildren().get(0);
		smallerTree.setParent(null);
		
		assertTrue(usersBids.getEntityTree().overlapsAtTopOf(bigTree));
		assertFalse(bidsItems.getEntityTree().overlapsAtTopOf(bigTree));
		assertFalse(itemsUsers.getEntityTree().overlapsAtTopOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().overlapsAtTopOf(bigTree));
		assertFalse(smallerTree.overlapsAtTopOf(bigTree));
	}
	
	@Test
	public void testOverlapsAtBottomOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree smallerTree = bigTree.getChildren().get(0).clone();
		smallerTree.setParent(null);
		
		assertFalse(usersBids.getEntityTree().overlapsAtBottomOf(bigTree));
		assertFalse(bidsItems.getEntityTree().overlapsAtBottomOf(bigTree));
		assertTrue(itemsUsers.getEntityTree().overlapsAtBottomOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().overlapsAtBottomOf(bigTree));
		assertTrue(smallerTree.overlapsAtBottomOf(bigTree));
	}
	
	@Test
	public void testGetOverlaps() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree smallerTree = bigTree.getChildren().get(0).clone();
		smallerTree.setParent(null);
		
		List<EntityTreeOverlap> overlap = smallerTree.overlapsIn(bigTree);
		assertTrue(overlap.size() == 1);
		assertTrue(overlap.get(0).getSize() == 3);
		assertTrue(overlap.get(0).getPartition().equalsEntireTree(smallerTree));
	}
	
	@Test
	public void testGetOverlapsMultiple() {
		// Users -> Bids -> Items -> Users
		EntityTree bigTree = usersBidsItemsUser.getEntityTree().clone();
		EntityTree bidsItemsUser = bigTree.getChildren().get(0);
		
		// Users -> Bids -> [ [Items -> Users] [Users] ]
		bidsItemsUser.addChild(new EntityTree(users, bigTree.getQueries()));
		
		EntityTree itemsUsers = bidsItemsUser.getChildren().get(0).clone();
		itemsUsers.setParent(null);
		
		List<EntityTreeOverlap> overlap = itemsUsers.overlapsIn(bigTree);
		
		assertTrue(overlap.size() == 1);
		assertTrue(overlap.get(0).getSize() == 2);
		assertTrue(overlap.get(0).getPartition().equalsEntireTree(itemsUsers));
	}
	
	/**
	 * Check for a partial match of:
	 * 
	 * Items -> Users in Bids -> Items -> Users
	 * Users -> Items in Bids->Items->Users
	 * 
	 */
	@Test
	public void testGetOverlapsPartial() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree().clone();
		EntityTree itemsUsersTree = itemsUsers.getEntityTree().clone();
		EntityTree bidsItemsUsersTree = bigTree.getChildren().get(0).clone();
		
		List<EntityTreeOverlap> overlap = itemsUsersTree.overlapsIn(bidsItemsUsersTree);
		assertEquals(overlap.size(), 1 );
		assertEquals(overlap.get(0).getSize(), 2);
		assertTrue(overlap.get(0).getPartition().equalsEntireTree(bidsItemsUsersTree.getRandomLeaf().getParent()));
		
		EntityTree usersItemsTree = usersItems.getEntityTree().clone();
		
		overlap = usersItemsTree.overlapsIn(bidsItemsUsersTree);
		assertTrue(overlap.size() == 1);
		assertTrue(overlap.get(0).getSize() == 1);
		assertTrue(overlap.get(0).getPartition().equalsEntireTree(bidsItemsUsersTree.getRandomLeaf()));
		assertTrue(overlap.get(0).getPartition().equalsNode(bidsItemsUsersTree.getRandomLeaf()));
	}
	
	@Test
	public void testGetLargestOverlap() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree itemsUsersTree = bigTree.getRandomLeaf().getParent().clone();
		itemsUsersTree.setParent(null);
		
		EntityTreeOverlap overlap = itemsUsersTree.overlapLargestIn(bigTree);
		
		assertTrue(overlap.getSize() == 2);
		assertTrue(overlap.getPartition().equalsEntireTree(itemsUsersTree));
	}
	
	@Test
	public void testGetLargestFromOverlaps() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree().clone();
		EntityTree bidsItemsUsersTree = bigTree.getRandomLeaf().getParent().getParent().clone();
		EntityTree bidsItemsTree = bidsItems.getEntityTree();
		
		bigTree.addChild(bidsItemsTree);
		
		List<EntityTreeOverlap> overlaps = bidsItemsUsersTree.overlapsIn(bigTree);
		
		assertTrue(overlaps.size() == 2);
		
		EntityTreeOverlap largestOverlap = bidsItemsUsersTree.overlapLargestIn(bigTree);
		assertTrue(largestOverlap.getPartition().equalsEntireTree(bidsItemsUsersTree));
		assertEquals(largestOverlap.getSize(), 3);
		
		largestOverlap = bidsItemsTree.overlapLargestIn(bigTree);
		assertTrue(largestOverlap.getPartition().equalsEntireTree(bigTree.getChildren().get(0)));
		assertEquals(largestOverlap.getPartition().size(), 3);
		assertEquals(largestOverlap.getSize(), 2);
	}
	
	@Test
	public void testGetQueryPathToParent() {
		// e.g. <Users>, <Bids> in [Users,Bids] at [Bids].
		EntityTree usersBidsTree = usersBids.getEntityTree();
		
	}
	
	@Test
	public void testGetTreePathToThisNode() {
		// e.g. UsersBidsItemsUsers and at Items should become => UsersBidsItems.
		EntityTree usersBidsItemsUsersTree = usersBidsItemsUser.getEntityTree();
		EntityTree treePath = usersBidsItemsUsersTree.getRandomLeaf().getParent().getTreePathToThisNode();
		
		System.out.println(usersBidsItemsUsersTree);
		System.out.println(treePath);
		
		assertEquals(treePath.size(), 3);
		assertEquals(treePath.getRandomLeaf(), usersBidsItemsUsersTree.getRandomLeaf().getParent());
	}
	
	@Test
	public void testQueriesGetSequences() {
		List<Query> queriesBids = bidsUsers.getEntityTree().getQueries();
		
		for(Query query: queriesBids) {
			List<Sequence> seqs = query.getSequences();
			
			assertTrue(seqs.contains(bidsUsers));
		}
		
		for(Query query: bidsItems.getEntityTree().getQueries()) {
			assertTrue(query.getSequences().contains(bidsUsers));
		}
	}
	
	@Test
	public void testSequenceIsConnected() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		EntityTree usersBidsTree = usersBids.getEntityTree();
		EntityTree bidsTree = usersBidsTree.getRandomLeaf().clone();
		EntityTree itemsUsersTree = itemsUsers.getEntityTree();
		
		assertTrue(usersBidsItemsUser.isSequenceConnectedBetween(bidsTree, itemsUsersTree));
		assertFalse(usersBidsItemsUser.isSequenceConnectedBetween(itemsUsersTree, bidsTree));
	}
	
	/**
	 * TODO:
	 * more testing with more complex EntityTrees.
	 * Multiple leaf nodes, multiple sequences...
	 */
	@Test
	public void testConnectedAtTopOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		// Different sequences (!= connected)
		assertFalse(usersBids.getEntityTree().connectedAtTopOf(bigTree));
		assertFalse(bidsItems.getEntityTree().connectedAtTopOf(bigTree));
		assertFalse(itemsUsers.getEntityTree().connectedAtTopOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().connectedAtTopOf(bigTree));
		
		assertTrue(usersBids.getEntityTree().connectedAtTopOf(itemsUsers.getEntityTree()));
		
		// Split sequence of Big Tree.
		EntityTree usersBids = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		EntityTree bids = new EntityTree(bigTree.getChildren().get(0).getNode(), bigTree.getChildren().get(0).getQueries());
		usersBids.addChild(bids);
		
		EntityTree itemsUsers = bigTree.getChildren().get(0).getChildren().get(0);
		
		assertTrue(usersBids.connectedAtTopOf(itemsUsers));
		assertFalse(itemsUsers.connectedAtTopOf(usersBids));
	}
	
	@Test
	public void testConnectedAtBottomOf() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertFalse(usersBids.getEntityTree().connectedAtBottomOf(bigTree));
		assertFalse(bidsItems.getEntityTree().connectedAtBottomOf(bigTree));
		assertFalse(itemsUsers.getEntityTree().connectedAtBottomOf(bigTree));
		assertFalse(bidsUsers.getEntityTree().connectedAtBottomOf(bigTree));
		
		assertTrue(itemsUsers.getEntityTree().connectedAtBottomOf(usersBids.getEntityTree()));
		
		// Split sequence of Big Tree.
		EntityTree usersBids = new EntityTree(bigTree.getNode(), bigTree.getQueries());
		EntityTree bids = new EntityTree(bigTree.getChildren().get(0).getNode(), bigTree.getChildren().get(0).getQueries());
		usersBids.addChild(bids);
		
		EntityTree itemsUsers = bigTree.getChildren().get(0).getChildren().get(0);
		
		assertFalse(usersBids.connectedAtBottomOf(itemsUsers));
		assertTrue(itemsUsers.connectedAtBottomOf(usersBids));
	}
	
	@Test
	public void testConnectedTo() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertFalse(usersBids.getEntityTree().connectedTo(bigTree));
		assertFalse(bidsItems.getEntityTree().connectedTo(bigTree));
		assertFalse(itemsUsers.getEntityTree().connectedTo(bigTree));
		assertFalse(bidsUsers.getEntityTree().connectedTo(bigTree));
		
		assertTrue(itemsUsers.getEntityTree().connectedTo(usersBids.getEntityTree()));
		assertTrue(usersBids.getEntityTree().connectedTo(itemsUsers.getEntityTree()));
	}
	
	@Test
	public void testIsContained() {
		// Test relationships e.g. Items -> Users and cardinality.
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(bigTree.isAlwaysContainedByParent());
		// Users -> Always have bids 
		assertTrue(bigTree.getChildren().get(0).isAlwaysContainedByParent());
		// Bids -> Items always have items
		assertTrue(bigTree.getChildren().get(0).getChildren().get(0).isAlwaysContainedByParent());
		// Items -> Users (users dont always sell items)
		assertFalse(bigTree.getChildren().get(0).getChildren().get(0).getChildren().get(0).isAlwaysContainedByParent());
	}
	
	/**
	 * Observe what happens when we clone a sub tree.
	 * 
	 * Then try to add it again to another parent. 
	 * Impact on elementsIndex.
	 */
	@Test
	public void testGetElements() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree itemsUsersTree = bigTree.getRandomLeaf().getParent().clone();
		itemsUsersTree.setParent(null);
		
		assertEquals(bigTree.getElements().size(), 4);
		assertEquals(itemsUsersTree.getElements().size(), 2);
		
		
		bigTree.getChildren().get(0).addChild(itemsUsersTree);
		
		assertEquals(bigTree.getElements().size(), 6);
		
	}
	
	/**
	 * Calculate distance between parent and child.
	 * If the child does not exist, it should return -1.
	 * 
	 * Or if the child is not a child.
	 */
	@Test
	public void testGetDistance() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertEquals(bigTree.getDistance(bigTree.getRandomLeaf()), 3);
		assertEquals(bigTree.getRandomLeaf().getDistance(bigTree), -1);
	}
	
	@Test
	public void testGetQueryFirstOccurence() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		EntityTree itemsTree = bigTree.getFirstQueryOccurrence(createQuery(items));
		
		assertEquals(bigTree.getRandomLeaf().getParent(), itemsTree);
		assertEquals(bigTree, bigTree.getFirstQueryOccurrence(createQuery(users)));
		assertTrue(bigTree == bigTree.getFirstQueryOccurrence(createQuery(users)));
		assertTrue(bigTree.getRandomLeaf() != bigTree.getFirstQueryOccurrence(createQuery(users)));
		
		HashSet<Field> conditionalFields = new HashSet<Field>();
		HashSet<Field> selectFields = new HashSet<Field>();
		selectFields.add(new Field("items.id", "int"));
		Query selectQuery = createQuery(items, conditionalFields, selectFields);
		
		assertEquals(bigTree.getFirstQueryOccurrence(selectQuery), null);	
	}
	
	/**
	 * User has average of 3 bids, each bid is on a single item.
	 */
	@Test
	public void testGetNodeFrequency() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertEquals(bigTree.getNodeFrequency(), 1);
		assertEquals(bigTree.getChildren().get(0).getNodeFrequency(), bids.getRelationshipWith(users).getCardinalityEntityTargeting(users));
		assertEquals(bigTree.getChildren().get(0).getChildren().get(0).getNodeFrequency(),
				bids.getRelationshipWith(users).getCardinalityEntityTargeting(users) * 
						bids.getRelationshipWith(items).getCardinalityEntityTargeting(bids));
		
		assertEquals(bigTree.getChildren().get(0).getChildren().get(0).getNodeFrequency() ,3);
	}
	
	@Test
	public void testHasQueryPathThatIsConnected() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		LinkedList<Query> queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(users));
		queryPath.add(createQuery(bids));
		assertTrue(bigTree.hasQueryPath(queryPath, true));
		
		queryPath.add(createQuery(users));
		assertFalse(bigTree.hasQueryPath(queryPath, true));
		
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(bids));
		queryPath.add(createQuery(items));
		assertTrue(bigTree.hasQueryPath(queryPath, true));
		
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(items));
		queryPath.add(createQuery(bids));
		assertFalse(bigTree.hasQueryPath(queryPath, true));
	}
	
	@Test
	public void testHasQueryPathIndirectlyConnected() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		// Preconditions
		// Test-case; items always have bids. Therefore fully represented by bids...
		assertTrue(bigTree.getRandomLeaf().getParent().getParent().isAlwaysContainedByParent());
		assertTrue(bigTree.getRandomLeaf().getParent().isAlwaysContainedByParent());
		// Not every User has an item -> Therefore not always contained.
		assertFalse(bigTree.getRandomLeaf().isAlwaysContainedByParent());
		
		
		LinkedList<Query> queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(items));
		queryPath.add(createQuery(users));
		assertTrue(bigTree.hasQueryPath(queryPath, false));
		
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(users));
		queryPath.add(createQuery(items));
		assertTrue(bigTree.hasQueryPath(queryPath, false));
		
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(users));
		assertFalse(bigTree.getChildLeafs().get(0).hasQueryPath(queryPath, false));
	}
	
	
	
	/**
	 * If we let items occur twice in two separate branches, 
	 * there should be two query paths for answering the Items query.
	 * 
	 * TODO: Review
	 */
	@Test
	public void testGetQueryPaths() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		LinkedList<Query> queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(bids));
		
		assertEquals(bigTree.getQueryPaths(queryPath, true).size(), 1);
		assertEquals(bigTree.getChildren().get(0), bigTree.getQueryPaths(queryPath, true).get(0));
		
		assertTrue(bigTree.getChildren().get(0).isAlwaysContainedByParent());
		assertTrue(bigTree.getChildren().get(0).getChildren().get(0).isAlwaysContainedByParent());
		
		bigTree.addChild(bidsItems.getEntityTree());
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(items));
		assertEquals(bigTree.getQueryPaths(queryPath, true).size(), 2);
	}
	
	/**
	 * Test correct end nodes in a query path within the tree.
	 * 
	 */
	@Test
	public void testGetEndNodesOfQueryPath() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		LinkedList<Query> queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(bids));
		List<EntityTree> endNodes = bigTree.getEndNodesOfQueryPath(queryPath, 0, false);
		
		assertEquals(endNodes.size(), 1);
		assertEquals(endNodes.get(0), bigTree.getChildren().get(0));
		
		// Just users query
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(users));
		endNodes = bigTree.getEndNodesOfQueryPath(queryPath, 0, false);
		
		assertEquals(endNodes.size(), 1);
		
		// Items query
		queryPath = new LinkedList<Query>();
		queryPath.add(createQuery(items));
		endNodes = largeTree.getEndNodesOfQueryPath(queryPath, 0, false);
		
		assertTrue(itemsUsers.getEntityTree().getQueries().contains(queryPath.get(0)));
		assertEquals(largeTree.getChildren().get(1), itemsUsers.getEntityTree());
		assertFalse(largeTree.getChildren().get(1).isAlwaysContainedByParent());
		
		// Still 1; because not every user has an item.
		assertEquals(endNodes.size(), 1);
		
	}
	
	@Test
	public void testHasQuery() {
		EntityTree bigTree = usersBidsItemsUser.getEntityTree();
		
		assertTrue(bigTree.canQuery(createQuery(users)));
		assertFalse(bigTree.canQuery(createQuery(items)));
	}
	
	@Test
	public void testCompareTo() {
		assertEquals(usersBids.getEntityTree().compareTo(usersBids.getEntityTree()), 0);
		assertEquals(itemsUsers.getEntityTree().compareTo(usersBids.getEntityTree()), -1);
		
		assertEquals(usersBids.getEntityTree().getChildren().get(0).compareTo(bidsItems.getEntityTree()), 0);
	}
	
	
	@Test
	public void testEqualsEntireTree() {
		assertFalse(usersBids.getEntityTree().equalsEntireTree(itemsUsers.getEntityTree()));
		assertTrue(usersBids.getEntityTree().equalsEntireTree(usersBids.getEntityTree()));
		
		Sequence usersBids2 = new Sequence();
		usersBids2.addQuery(createQuery(users));
		usersBids2.addQuery(createQuery(bids));
		
		assertTrue(usersBids.getEntityTree().equalsEntireTree(usersBids2.getEntityTree()));
	}
	
	@Test
	public void testQueryEquals() {
		Query queryBids = createQuery(users);
		Query queryItems = createQuery(items);
		Query queryBids2 = createQuery(users);
		Query queryItems2 = createQuery(items);
		
		assertEquals(queryBids, queryBids2);
		assertEquals(queryItems, queryItems2);
		assertNotEquals(queryItems, queryBids);
		
		Sequence usersBids = new Sequence();
		usersBids.addQuery(queryBids);
		usersBids.addQuery(queryItems);
		
		Sequence usersBids2 = new Sequence();
		usersBids2.addQuery(queryBids2);
		usersBids2.addQuery(queryItems2);
		
		assertEquals(queryBids, queryBids2);
		assertEquals(queryItems, queryItems2);
		
		assertEquals(bidsUsers.getQueryPath().getFirst(), bidsItems.getQueryPath().getFirst());
		assertEquals(bidsUsers.getEntityTree().getQueries(), bidsItems.getEntityTree().getQueries());
		assertNotEquals(usersBids.getQueryPath().getFirst(), bidsItems.getQueryPath().getFirst());
	}
	
	@Test
	public void testEqualsHashCode() {
		assertNotEquals(usersBids.getEntityTree().hashCode(),itemsUsers.getEntityTree().hashCode());
		assertEquals(usersBids.getEntityTree().hashCode(),usersBids.getEntityTree().hashCode());
		
		Sequence usersBids2 = new Sequence();
		usersBids2.addQuery(createQuery(users));
		usersBids2.addQuery(createQuery(bids));
		
		assertEquals(usersBids.getEntityTree().hashCode(),usersBids2.getEntityTree().hashCode());
	}
}
