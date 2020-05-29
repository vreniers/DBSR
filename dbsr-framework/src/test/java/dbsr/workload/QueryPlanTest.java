package dbsr.workload;


import static org.junit.Assert.*;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import dbsr.candidate.CandidateSequence;
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

public class QueryPlanTest {
	
	private Sequence usersBidsItemsSeller;
	
	private Sequence usersBidsItemsUsers;
	
	private Sequence usersRegions;
	
	private Sequence bidsUsers;
	
	private Sequence bidsItems;
	
	private Sequence itemsBids;
	
	private Sequence itemsUsers;
	
	private Sequence usersItems;
	
	private Set<Sequence> sequences;
	
	private Entity users, items, bids, regions;
	
	// For usersBidsItemsSellers.
	private LinkedList<CandidateSequence> candidates;
	
	private QueryPlan<CandidateSequence> queryPlan;
	
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
	
	@Before
	public void setUp() throws Exception {
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
		
		Query usersQuery = createQuery(users);
		Query bidsQuery = createQuery(bids);
		Query itemsQuery = createQuery(items);
		Query usersTwoQuery = createQuery(users);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		usersBidsItemsSeller.addQuery(createQuery(users, conditionalFields));
		
		// --- usersBidsItemsUsers---
		usersBidsItemsUsers = new Sequence();
		
		usersBidsItemsUsers.addQuery(usersQuery);
		usersBidsItemsUsers.addQuery(bidsQuery);
		usersBidsItemsUsers.addQuery(itemsQuery);
		usersBidsItemsUsers.addQuery(createQuery(users));
		
		// --- ItemsBids ---
		itemsBids = new Sequence();
		itemsBids.addQuery(itemsQuery);
		itemsBids.addQuery(bidsQuery);
		
		
		// --- BidsItems ---
		bidsItems = new Sequence();
		bidsItems.addQuery(bidsQuery);
		
		conditionalFields = new HashSet<Field>();
		HashSet<Field> selectFields = new HashSet<Field>();
		selectFields.add(new Field("items.id", "int"));
		bidsItems.addQuery(createQuery(items, conditionalFields, selectFields));
		
		// --- ItemsUsers ---
		itemsUsers = new Sequence();
		itemsUsers.addQuery(itemsQuery);
		
		selectFields = new HashSet<Field>();
		selectFields.add(new Field("users.id", "int"));
		itemsUsers.addQuery(createQuery(users, conditionalFields, selectFields));
		
		usersItems = new Sequence();
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(itemsQuery);
		
		bidsUsers = new Sequence();
		bidsUsers.addQuery(bidsQuery);
		bidsUsers.addQuery(usersQuery);
		
		usersRegions = new Sequence();
		usersRegions.addQuery(usersQuery);
		usersRegions.addQuery(createQuery(regions));
		
		sequences = new HashSet<Sequence>();
		sequences.add(usersBidsItemsSeller);
		sequences.add(usersRegions);
		sequences.add(bidsUsers);
		sequences.add(bidsItems);
		sequences.add(itemsBids);
		sequences.add(itemsUsers);
		sequences.add(usersItems);
		
		createCandidates();
	}
	
	public void createCandidates() {
		candidates = new LinkedList<CandidateSequence>();
		
		for(Query query: usersBidsItemsSeller.getQueryPath()) {
			ArrayList<Query> queries = new ArrayList<Query>();
			queries.add(query);
			
			EntityTree node = new EntityTree(query.getEntity(), queries);
			CandidateSequence candidate = new CandidateSequence(node, usersBidsItemsSeller);
			candidates.add(candidate);
		}
		
		this.queryPlan = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
	}
	
	@Test
	public void testQueryMappingInitialization() {
		QueryPlan<CandidateSequence> qp = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		assertEquals(qp.getMapping().size(), 4);
	}
	
	@Test
	public void testQueryPlanHashCode() throws Exception {
		
		QueryPlan<CandidateSequence> qp = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		createCandidates();
		
		QueryPlan<CandidateSequence> qp2 = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		assertEquals(qp, qp2);
		assertEquals(qp.hashCode(), qp2.hashCode());
		
		setUp();
		QueryPlan<CandidateSequence> qp3 =  new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		assertEquals(qp, qp3);
		assertEquals(qp.hashCode(), qp3.hashCode());
	}
	
	@Test
	public void testSupersededBy() {
		QueryPlan<CandidateSequence> qp = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		createCandidates();
		
		QueryPlan<CandidateSequence> qp2 = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		assertFalse(qp.supersededBy(qp2));
		assertFalse(qp2.supersededBy(qp));
		
		candidates.add(candidates.get(1));
		assertTrue(qp2.supersededBy(qp));
		assertFalse(qp.supersededBy(qp2));
	}
	
	/**
	 * QueryPlan:
	 * Users -> Bids -> Items -> Users
	 * 
	 * Present candidate:
	 * [Users|Bids]
	 * 
	 * Two new QPS:
	 * [Users|Bids] -> Items -> Users (result of compaction was originally [Users] -> [Users|Bids] -> ..)
	 */
	@Test
	public void testNotifyNewCandidate() {
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		
		System.out.println("start: " + queryPlan);
		Set<QueryPlan<CandidateSequence>> newQPs = queryPlan.notifyNewCandidate(usersBids);
		
//		assertEquals(newQPs.size(), 1);
		
		
		System.out.println("generated--");
		for(QueryPlan<CandidateSequence> qp: newQPs) {
			System.out.println(qp);
			if(qp.size() == 3) {
				// First candidate is: [Users|Bids] 
				qp.getCandidatesPlan().get(0).getCandidate().equalsNode(candidates.get(0).getCandidate());
				qp.getCandidatesPlan().get(0).getCandidate().getRandomLeaf().equalsNode(candidates.get(1).getCandidate());
			}
		}
	}
	
	/**
	 * QueryPlan:
	 * Users -> Bids -> Items -> Users2
	 * 
	 * Present candidate:
	 * [Users|Bids]
	 * 
	 * Two new QPS:
	 * [Users|Bids] -> Items -> Users (result of compaction was originally [Users] -> [Users|Bids] -> ..)
	 * 
	 * Test: no indexes, since [Users|Bids] is known by user id. 
	 * And [Users|Bids] in last query is a search on just the users.
	 */
	@Test
	public void testSecondaryIndexCompaction() {
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		
		Set<QueryPlan<CandidateSequence>> newQPs = queryPlan.notifyNewCandidate(usersBids);
		
//		assertEquals(newQPs.size(), 2);
		
		for(QueryPlan<CandidateSequence> qp: newQPs) {
//			System.out.println(qp);
			assertTrue(qp.getSecondaryIndexes().values().isEmpty());
		}
	}
	
	/**
	 * Users -> Bids -> Items -> Users2
	 * 
	 * Candidate: [Users | Bids| Items]
	 * 
	 * Should result in one query with two candidates.
	 * 
	 */
	@Test
	public void testSecondaryIndexCreationDissolves() {
		CandidateSequence usersItems = candidates.get(0).merge(candidates.get(1)).merge(candidates.get(2));
		
		Set<QueryPlan<CandidateSequence>> newQPs = queryPlan.notifyNewCandidate(usersItems);
		
		for(QueryPlan<CandidateSequence> qp: newQPs) {
//			System.out.println(qp);
			assertEquals(qp.size(), 2);
			assertEquals(qp.getCandidatesPlan().get(0), usersItems);
			assertTrue(qp.getSecondaryIndexes().values().isEmpty());
		}
	}
	
	@Test
	public void testSecondaryIndexCreation() {
		CandidateSequence bidsItems = candidates.get(1).merge(candidates.get(2));
		
		// Users -> Bids -> Items -> Users
		Set<QueryPlan<CandidateSequence>> newQPs = queryPlan.notifyNewCandidate(bidsItems);
		
		// Users -> Items NOTIFY existence of [Bids|Items] creates secondary index for Items
		LinkedList<CandidateSequence> candidatesTwo = new LinkedList<CandidateSequence>();
		EntityTree usersTree = new EntityTree(users, usersItems.getEntityTree().getQueries());
		EntityTree itemsTree = usersItems.getEntityTree().getRandomLeaf().clone();
		candidatesTwo.add(new CandidateSequence(usersTree, usersItems));
		candidatesTwo.add(new CandidateSequence(itemsTree, usersItems));
		
		QueryPlan<CandidateSequence> qpTwo = new QueryPlan<CandidateSequence>(usersItems, candidatesTwo);
		
		newQPs = qpTwo.notifyNewCandidate(bidsItems);
		
		assertEquals(newQPs.size(), 1);
		
		for(QueryPlan<CandidateSequence> qp: newQPs) {
			assertFalse(qp.getSecondaryIndexes().isEmpty());
			assertEquals(qp.getSecondaryIndexes().size(), 1);
		}
		
		// Cand: [Users| Items] => No sec. index.
		CandidateSequence usersItems = candidates.get(0).merge(candidates.get(2));
		newQPs = qpTwo.notifyNewCandidate(usersItems);
		
		for(QueryPlan<CandidateSequence> qp: newQPs) {
			assertTrue(qp.getSecondaryIndexes().isEmpty());
		}
	}
	
	/**
	 * Tests shift operation when removing an index and candidate.
	 * 
	 * Users -> Bids -> Items -> Users
	 * 
	 * Remove bids and index of Items shifts one lower.
	 */
	@Test
	public void testRemoveCandidate() {
		QueryPlan<CandidateSequence> qp = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		System.out.println(qp);
		
		System.out.println(qp.getMapping());
		assertEquals(4, qp.getMapping().keySet().size());
		assertEquals(qp.getMapping().get(0).getFirst(), qp.getSequence().getQueryPath().getFirst());
		assertTrue(qp.getMapping().containsKey(new Integer(3)));
		assertTrue(!qp.getMapping().get(new Integer(3)).isEmpty());
		
		qp.removeCandidateAndTo(3, 2);
		
		assertEquals(qp.getMapping().keySet().size(), 3);
		assertFalse(qp.getMapping().containsKey(new Integer(3)));
		
		System.out.println(qp);
	}
	
	/**
	 * Attributes have to be connected which are selected.
	 * E.g. Users and Bids are selected at Candidate 2.
	 * Must occur in a path in the candidate tree.
	 * 
	 * E.g. Items -> Users -> Bids -> ...
	 * 
	 * TODO: e.g. test a QP with no mapped queries.
	 */
	@Test
	public void testIsValidQueryPlan() {
		// Test if one candidate has no mapped queries.
		
		
	}
}