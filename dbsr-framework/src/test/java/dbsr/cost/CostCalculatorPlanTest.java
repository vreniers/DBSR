package dbsr.cost;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbsr.candidate.CandidateSequences;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.model.tree.EntityTree;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class CostCalculatorPlanTest {


	private Sequence usersBidsItemsSeller;
	
	private Sequence bidsUsers;
	
	private Sequence bidsItems;
	
	private Sequence itemsBids;
	
	private Sequence itemsUsers;
	
	private Sequence usersBids;
	
	private Sequence usersBidsItems;
	
	private Sequence usersItemsBids;
	
	// Has extra user field.
	private Sequence usersBidsTwo;
	
	private Sequence usersItems;
	
	private Entity users, items, bids, regions;
	
	// Normalized candidate structures.
	private CandidateSequences usersCandidate, bidsCandidate, itemsCandidate; 
	
	// Candidate structures
	private CandidateSequences usersBidsItemsUsersCandidate;
	
	private CandidateSequences usersBidsCandidate;
	
	private CandidateSequences usersBidsExtraFieldCandidate;
	
	// Users|Items|Bids
	private CandidateSequences usersItemsBidsCandidate;
	
	private CandidateSequences itemsBidsCandidate;
	
	// Bids|Items
	private CandidateSequences bidsItemsCandidate;
		
	// Query plans	
	private QueryPlan<CandidateSequences> qpUsersBidsJoinNormalized;
	
	private QueryPlan<CandidateSequences> qpUsersBidsSingle;
	
	private QueryPlan<CandidateSequences> qpUsersBidsSingleExtraField;
	
	// Users|Bids|Items|Users
	private QueryPlan<CandidateSequences> qpUsersBidsSingleOnFull;
	
	// UsersBids on twice Users|Bids|Items|Users.
	private QueryPlan<CandidateSequences> qpUsersBidsJoin;
	
	// UsersBidsItems on Users|Bids|Items|Users (Q0:Users,Bids) and Users|Bids|Items|Users (Q1:Items)
	private QueryPlan<CandidateSequences> qpUsersBidsItemsJoin;
	
	// UsersBidsItems on Users|Bids|Items|Users (Q0:Users,Bids) and Bids|Items (Q1:Items)
	private QueryPlan<CandidateSequences> qpUsersBidsItemsJoinTwo;
	
	// UsersBidsItems on Users|Bids|Items|Users (Q0:Users,Bids) and Items (Q1:Items)
	private QueryPlan<CandidateSequences> qpUsersBidsItemsJoinThree;
	
	// ItemsBids on [Users|Items|Bids]
	private QueryPlan<CandidateSequences> qpItemsBidsIndex;
	
	// ItemsBids on [Items|Bids]
	private QueryPlan<CandidateSequences> qpItemsBids;
	
	// ItemsBids on [Bids|Items]
	private QueryPlan<CandidateSequences> qpItemsBidsIndexReversed;
	
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		createSequences();
		
		createCandidates();
		
		createQueryPlans();		
	}
	
	private void createCandidates() {
		// Normalized candidates
		LinkedList<Query> queries = new LinkedList<Query>();
		queries.add(createQuery(users));
		usersCandidate = new CandidateSequences(EntityTree.createEntityTree(queries));
		
		queries = new LinkedList<Query>();
		queries.add(createQuery(bids));
		bidsCandidate = new CandidateSequences(EntityTree.createEntityTree(queries)); 
		
		queries = new LinkedList<Query>();
		queries.add(createQuery(items));
		itemsCandidate = new CandidateSequences(EntityTree.createEntityTree(queries));
		
		// Candidate structures
		usersBidsItemsUsersCandidate = new CandidateSequences(usersBidsItemsSeller.getEntityTree());
		usersBidsCandidate = new CandidateSequences(usersBids.getEntityTree());
		usersBidsExtraFieldCandidate = new CandidateSequences(usersBidsTwo.getEntityTree());
		
		bidsItemsCandidate = new CandidateSequences(bidsItems.getEntityTree());
		itemsBidsCandidate = new CandidateSequences(itemsBids.getEntityTree());
		usersItemsBidsCandidate = new CandidateSequences(usersItemsBids.getEntityTree());
	}
	
	private void createSequences() {
		usersBidsItemsSeller = new Sequence();
		
		users = createEntity("users");
		items = createEntity("items");
		bids = createEntity("bids");
		regions = createEntity("regions");
		
		Relationship usersPlaceBids = new Relationship("usersPlaceBids", users, bids, RelationshipType.OneToMany);
		Relationship bidsAreOnItems = new Relationship("bidsAreOnItems", bids, items, RelationshipType.ManyToOne);
		Relationship usersSellItems = new Relationship("usersSellItems", users, items, RelationshipType.OneToMany);
		//Relationship itemsHaveSeller = new Relationship("itemHasSeller", items, users, RelationshipType.ManyToOne); //EQUI
		
		Relationship usersHaveRegion = new Relationship("usersHasRegion", users, regions, RelationshipType.ManyToOne);
		
		// Set cardinalities
		usersPlaceBids.setCardinality(new Cardinality(1,3));
		bidsAreOnItems.setCardinality(new Cardinality(10,1));
		usersSellItems.setCardinality(new Cardinality(1,5));
		usersHaveRegion.setCardinality(new Cardinality(1000,1));
				
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		
		Query usersQuery = createQuery(users);
		Query bidsQuery = createQuery(bids);
		Query itemsQuery = createQuery(items);		
		Query usersTwoQuery = createQuery(users, conditionalFields);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersTwoQuery);
		
		// --- usersBidsItems -------
		usersItemsBids = new Sequence();
		usersItemsBids.addQuery(usersQuery);
		usersItemsBids.addQuery(itemsQuery);
		usersItemsBids.addQuery(bidsQuery);
		
		// --- UsersBids ---
		usersBids = new Sequence();
		usersBids.addQuery(usersQuery);
		usersBids.addQuery(bidsQuery);
		
		// Extra user field
		usersBidsTwo = new Sequence();
		usersBidsTwo.addQuery(usersTwoQuery);
		usersBidsTwo.addQuery(bidsQuery);
		
		// --- UsersBidsItems ---
		usersBidsItems = new Sequence();
		usersBidsItems.addQuery(usersQuery);
		usersBidsItems.addQuery(bidsQuery);
		usersBidsItems.addQuery(itemsQuery);
				
		// --- ItemsBids ---
		itemsBids = new Sequence();
		itemsBids.addQuery(itemsQuery);
		itemsBids.addQuery(bidsQuery);
		
		
		// --- BidsItems ---
		bidsItems = new Sequence();
		bidsItems.addQuery(bidsQuery);
		bidsItems.addQuery(createQuery(items));
		
		// --- ItemsUsers ---
		itemsUsers = new Sequence();
		itemsUsers.addQuery(itemsQuery);
		
		HashSet<Field> selectFields = new HashSet<Field>();
		selectFields = new HashSet<Field>();
		selectFields.add(new Field("users.id", "int"));
		itemsUsers.addQuery(createQuery(users, conditionalFields, selectFields));
		
		usersItems = new Sequence();
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(itemsQuery);
		
		bidsUsers = new Sequence();
		bidsUsers.addQuery(bidsQuery);
		bidsUsers.addQuery(usersQuery);
	}
	
	private void createQueryPlans() {
		// Normalized query plan: Users -> Bids on [Users]  and [Bids].
		LinkedList<CandidateSequences> candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersCandidate);
		candidatesOrder.add(bidsCandidate);
		HashMap<Integer, LinkedList<Query>> queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), new LinkedList<Query>(usersBids.getQueryPath().subList(0,1)));
		queryDataMapping.put(new Integer(1), new LinkedList<Query>(usersBids.getQueryPath().subList(1,2)));
		qpUsersBidsJoinNormalized = new QueryPlan<CandidateSequences>(usersBids, candidatesOrder, queryDataMapping);
				
		// Query plan UsersBids on [Users|Bids]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), usersBids.getQueryPath());
		qpUsersBidsSingle = new QueryPlan<CandidateSequences>(usersBids, candidatesOrder, queryDataMapping);
		
		// Query plan UsersBidsTwo on [Users(xtraField)|Bids]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsExtraFieldCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), usersBidsTwo.getQueryPath());
		qpUsersBidsSingleExtraField = new QueryPlan<CandidateSequences>(usersBidsTwo, candidatesOrder, queryDataMapping);
		
		// Query plan UsersBids on [Users|Bids|Items|Users]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), usersBids.getQueryPath());
		qpUsersBidsSingleOnFull = new QueryPlan<CandidateSequences>(usersBids, candidatesOrder, queryDataMapping);
		
		// Query plan usersBidsItems on [Users|Bids|Items|Users] (Q0:Users,BIds) and [Users|Bids|Items|Users] (Q1: Items)
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		queryDataMapping = new HashMap<Integer,LinkedList<Query>>();
		queryDataMapping.put(new Integer(0),  new LinkedList<Query>(usersBidsItems.getQueryPath().subList(0, 2)));
		queryDataMapping.put(new Integer(1), new LinkedList<Query>(usersBidsItems.getQueryPath().subList(2, 3)));
		qpUsersBidsItemsJoin = new QueryPlan<CandidateSequences>(usersBidsItems, candidatesOrder, queryDataMapping);
		
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		candidatesOrder.add(bidsItemsCandidate);
		queryDataMapping = new HashMap<Integer,LinkedList<Query>>();
		queryDataMapping.put(new Integer(0),  new LinkedList<Query>(usersBidsItems.getQueryPath().subList(0, 2)));
		queryDataMapping.put(new Integer(1), new LinkedList<Query>(usersBidsItems.getQueryPath().subList(2, 3)));
		qpUsersBidsItemsJoinTwo = new QueryPlan<CandidateSequences>(usersBidsItems, candidatesOrder, queryDataMapping);
		
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		candidatesOrder.add(itemsCandidate);
		queryDataMapping = new HashMap<Integer,LinkedList<Query>>();
		queryDataMapping.put(new Integer(0),  new LinkedList<Query>(usersBidsItems.getQueryPath().subList(0, 2)));
		queryDataMapping.put(new Integer(1), new LinkedList<Query>(usersBidsItems.getQueryPath().subList(2, 3)));
		qpUsersBidsItemsJoinThree = new QueryPlan<CandidateSequences>(usersBidsItems, candidatesOrder, queryDataMapping);
		
		
		// Index: Query Items -> Bids in [Users|Items|Bids]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersItemsBidsCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), itemsBids.getQueryPath());
		qpItemsBidsIndex = new QueryPlan<CandidateSequences>(itemsBids, candidatesOrder, queryDataMapping);
		
		// Query Items -> Bids in [Items|Bids]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(itemsBidsCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), itemsBids.getQueryPath());
		qpItemsBids = new QueryPlan<CandidateSequences>(itemsBids, candidatesOrder, queryDataMapping);
		
		// JOIN: Query Items -> Bids in [Items] and [Bids] (sort of)
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		candidatesOrder.add(usersBidsItemsUsersCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), new LinkedList<Query>(usersBids.getQueryPath().subList(0, 1)));
		queryDataMapping.put(new Integer(1), new LinkedList<Query>(usersBids.getQueryPath().subList(1,2)));
		qpUsersBidsJoin = new QueryPlan<CandidateSequences>(usersBids, candidatesOrder, queryDataMapping);
		
		// Reverse Index: Query Items -> Bids in [Bids|Items]
		candidatesOrder = new LinkedList<CandidateSequences>();
		candidatesOrder.add(bidsItemsCandidate);
		queryDataMapping = new HashMap<Integer, LinkedList<Query>>();
		queryDataMapping.put(new Integer(0), itemsBids.getQueryPath());
		qpItemsBidsIndexReversed = new QueryPlan<CandidateSequences>(itemsBids, candidatesOrder, queryDataMapping);
		
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
		return createQuery(entity, new HashSet<Field>());
	}
	
	private static Query createQuery(Entity entity, Set<Field> conditionalFields) {
		Query query = new SelectQuery(entity, new Integer(10), entity.getFields(), conditionalFields);
		
		return query;
	}
	
	private static Query createQuery(Entity entity, Set<Field> conditionalFields, Set<Field> selectFields) {
		Query query = new SelectQuery(entity, new Integer(10), selectFields, conditionalFields);
		
		return query;
	}

	/**
	 * Query: Users -> Bids in [Users|Bids]
	 * Query: Users -> Bids in [Users|Bids|Items|User]
	 * 
	 * Cardinality: Users 1 - 3 bids.
	 */
	@Test
	public void testGetCostQueryPlan() {
		int cost = qpUsersBidsSingle.getCost();
		int costTwo = qpUsersBidsSingleOnFull.getCost();
		
		// costTwo selects a lot more fields due to the selected data structure.
		assertTrue(cost < costTwo);
		assertTrue(cost > 1);
		
		int costThree = qpUsersBidsSingleExtraField.getCost();
//		System.out.println(costThree);
		assertTrue(cost < costThree);
		
		// costThree selects 1 more field than cost, so should be 1 more.
		assertTrue((cost+1) == costThree);
	}
	
	/**
	 * Query: Users -> Bids on [Users|Bids|Items|User],[Users|Bids|Items|User[
	 * 
	 */
	@Test
	public void testGetCostQueryPlanJoin() {		
		int cost = qpUsersBidsSingle.getCost();
		int costJoin = qpUsersBidsJoin.getCost();	
		
		int costJoinNormalized = qpUsersBidsJoinNormalized.getCost();
		
//		System.out.println(cost);
//		System.out.println(costJoin);
//		System.out.println(costJoinNormalized);
//		
//		System.out.println(qpUsersBidsJoinNormalized);
//		System.out.println(qpUsersBidsJoin);
//		
		// !!! costJoin can be less than costJoinNormalized because!
		// costJoin only has to do 1 second look up. Whereas costJoinNormalized joins with multiple records(!).
		
		assertTrue(cost < costJoin);
		assertTrue(cost < costJoinNormalized);
		
		// Hard to compare costJoin vs costJoinNormalized depends on the cost weight set of a record look-up.
	}
	
	/** 
	 * Index Case A: Not-reversed.
	 * 
	 * Query: Item -> Bids in [Users|Items|Bids]
	 * Query: Item -> Bids in [Items|Bids]
	 * 
	 * User ... Items = 1 ... 5
	 * 
	 * 1.A) Users(1)-> Items(5) -> Bids
	 *      Results in 1 query per Item.
	 *      
	 **/
	@Test
	public void testGetCostQueryPlanIndex() {
		assertFalse(qpItemsBids.hasSecondaryIndex());
		assertTrue(qpItemsBidsIndex.hasSecondaryIndex());
		
		
		int costWithIndex = qpItemsBidsIndex.getCost();
		int costWithoutIndex = qpItemsBids.getCost();
		
		assertTrue(costWithIndex > costWithoutIndex);
		
		int freq = usersItemsBids.getEntityTree().getChildren().get(0).getNodeFrequency();
		
		System.out.println(freq);
		System.out.println(costWithIndex);
		System.out.println(costWithoutIndex);
		
	}
	
	/**
	 * For example
	 *  Users -> Bids on [Users] and [Bids]. 
	 *  
	 *  The frequency of bids selected should be equal to the cardinality of Bids per User. 
	 *  
	 */
	@Test
	public void testJoinFrequencyCost() {
		LinkedList<Integer> frequencies = qpUsersBidsJoinNormalized.getSelectFrequencies();
		
		// This should match with the cardinalities.
		// Starting with 1, since we only select one parent record, and then followed by the number of Bids a user typically has.		
		assertTrue(frequencies.get(0) == 1);
		assertTrue(frequencies.get(1) == users.getRelationshipWith(bids).getCardinalityEntityTargeting(users));
	}
	
	/**
	 * If some data has been queried in the past, the secondary index is shifted upwards to take
	 * this into account. In addition, the join frequency is altered for the subsequent candidate.
	 * 
	 * For example:
	 * 	Users -> Bids is queried on [Users] and [Users|Bids]. The number of selected records (frequeny)
	 *  on [Users|Bids] is the frequency of Users. And not the cardinality of Bids between Users.
	 * 
	 * 
	 * Test 1.) We use Users->Bids on [Users|Bids|Items|Users], [Users|Bids|Items|Users] as test.
	 * 
	 * Test 2.) Deeper past. 
	 * 			E.g. Users-Bids->Items on [Users|Bids|Items|Users],  [Users|Bids|Items|Users]
	 */
	@Test
	public void testQueriesPastAndFrequency() {
//		System.out.println(qpUsersBidsJoin);
//		System.out.println(users.getRelationshipWith(bids).getCardinalityEntityTargeting(users));
//		System.out.println(bids.getRelationshipWith(items).getCardinalityEntityTargeting(items));
//		
		LinkedList<Integer> frequencies = qpUsersBidsJoin.getSelectFrequencies();
		
		// This should match with the cardinalities.
		// Starting with 1, since we only select one parent record, and then ggain folowed by one, since we select the same User record over.	
		assertTrue(frequencies.get(0) == 1);
		assertTrue(frequencies.get(1) == 1);
		
		// Users->Bids->Items on [Users|Bids|Items|Users] (Q:0 Users,Bids]), [Users|Bids|Items|Users] (Q1:Items)
		frequencies = qpUsersBidsItemsJoin.getSelectFrequencies();
		assertTrue(frequencies.get(0) == 1);
		assertTrue(frequencies.get(1) == 1);
		
		// Users->Bids->Items on [Users|Bids|Items|Users] and [Bids|Items]  (Q0:Users,Bids) and Q1(Items) 
		//  Cardinality between Users and Bids
		frequencies = qpUsersBidsItemsJoinTwo.getSelectFrequencies();
		
		assertTrue(frequencies.get(0) == 1);
		assertTrue(frequencies.get(1) == users.getRelationshipWith(bids).getCardinalityEntityTargeting(users));
		
		// Users->Bids->Items on [Users|Bids|Items|Users] and [Items].
		frequencies = qpUsersBidsItemsJoinThree.getSelectFrequencies();
		
		assertTrue(frequencies.get(0) == 1);
		assertTrue(frequencies.get(1) == bids.getRelationshipWith(items).getCardinalityEntityTargeting(bids));
	}

	
	
	/**
	 * Index Case B: Reversed
	 * 
	 * Query: Items -> Bids in [Bids|Items]
	 * 
	 * 1.A) Bids | Items  10...1
	 * 
	 * TODO: Fix cost is still a bit too low.
	 */
	@Test
	public void testGetCostQueryPlanReversedIndex() {

//		System.out.println(qpItemsBidsIndexReversed.getCandidatesPlan());
//		System.out.println(qpItemsBidsIndexReversed.getSecondaryIndexes());
		
		assertTrue(qpItemsBidsIndexReversed.hasSecondaryIndex());
		
		
		int cost = qpItemsBidsIndexReversed.getCost();
		
//		System.out.println(cost);
	}
	
	@Test
	public void testGetCostQueryPlanReversedSecondaryIndex() {
		assertTrue(qpItemsBidsIndexReversed.hasSecondaryIndex());
	}

}
