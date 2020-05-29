package dbsr.candidates;

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

public class CandidateSequenceTest {
	
	private Sequence usersBidsItemsSeller;
	
	private Entity users, bids, items;
	
	private LinkedList<CandidateSequence> candidates;
	
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
	
	@Before
	public void setUp() throws Exception {
		usersBidsItemsSeller = new Sequence();
		
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
		
		usersBidsItemsSeller.addQuery(createQuery(users));
		usersBidsItemsSeller.addQuery(createQuery(bids));
		usersBidsItemsSeller.addQuery(createQuery(items));
		usersBidsItemsSeller.addQuery(createQuery(users));
	}
	
	@Before
	public void testCandidateCreation() {
		candidates = new LinkedList<CandidateSequence>();
		
		for(Query query: usersBidsItemsSeller.getQueryPath()) {
			ArrayList<Query> queries = new ArrayList<Query>();
			queries.add(query);
			
			EntityTree node = new EntityTree(query.getEntity(), queries);
			CandidateSequence candidate = new CandidateSequence(node, usersBidsItemsSeller);
			candidates.add(candidate);
		}
	}
	
	@Test
	public void candidateSize() {
		assertEquals(candidates.size(), 4);
	}
	
	/**
	 * Test merges of single candidate entities in a sequence.
	 */
	@Test
	public void testCanMerge() {
		assertTrue(candidates.get(0).canMerge(candidates.get(1)));
		assertTrue(candidates.get(1).canMerge(candidates.get(2)));
		assertTrue(candidates.get(2).canMerge(candidates.get(3)));
		
		assertFalse(candidates.get(0).canMerge(candidates.get(3)));
	}
	
	@Test
	public void testMerge() {
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		CandidateSequence bidsItems = candidates.get(1).merge(candidates.get(2));
		CandidateSequence itemsUsers = candidates.get(2).merge(candidates.get(3));
		
		assertTrue(usersBids.getCandidate().getNode().equals(users));
		assertTrue(usersBids.getCandidate().getRandomLeaf().getNode().equals(bids));
		assertTrue(bidsItems.getCandidate().getNode().equals(bids));
		assertTrue(bidsItems.getCandidate().getRandomLeaf().getNode().equals(items));
		assertTrue(itemsUsers.getCandidate().getNode().equals(items));
		assertTrue(itemsUsers.getCandidate().getRandomLeaf().getNode().equals(users));
	}
	
	@Test
	public void testQueryPlanOptimize() {
		QueryPlan<CandidateSequence> queryPlan = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		Set<CandidateSequence> newCandidates = queryPlan.optimize();
		
		assertTrue(newCandidates.size() == 3);
	}
	
	/**
	 * Users -> Bids -> Items -> User (Seller)
	 * 
	 * Candidate: [Users | Bids]
	 * 
	 * First outcomes:
	 * - [Users | Bids] -> Bids -> Items -> User  (S)
	 * - Users -> Bids -> Items -> [Users|Bids]   (S)
	 * - Users -> [Users | Bids] -> Items -> User (C)
	 * 
	 * Candidate occurs multiple times and fully contained? => Replace once = Substitute rule (S+S)
	 * Compact possible => Compaction rule => new 3rd QP
	 * 
	 * QP1 [Users | Bids] -> Bids -> Items -> [Users |Bids] (S)
	 * QP2 Users -> [Users | Bids] -> Items -> User 
	 * QP3 [Users | Bids] -> Items -> [User | Bids] (C)
	 * 
	 * Keep QP1 and QP2 for redundancy.
	 * 
	 */
	@Test
	public void testQueryPlanCreation() {
		QueryPlan<CandidateSequence> queryPlan = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		
		System.out.println(usersBids);
		Set<QueryPlan<CandidateSequence>> qps = queryPlan.notifyNewCandidate(usersBids);
		
		for(QueryPlan<CandidateSequence> qp: qps) {
			System.out.println(qp);
		}
	}
	
	/**
	 * TODO: FIX SUBSCRIBERS, RELATIONSHIPS A -> B.
	 * [A|B] => ADD SUBSCRIBERS TO SUB(A) for [A|B]
	 * [A|B] => SUB(B) only if A->B is GUARANTUEED.
	 * 
	 * Users -> Bids -> Items -> Users (Seller)
	 * 
	 * Candidate: [Items | Users]
	 * 
	 * Current outcomes:
	 * Users -> Bids -> [Items | Users] -> Users
	 * Users -> Bids -> [Items | Users]
	 * 
	 * 
	 */
	@Test
	public void testQueryPlanCreation2() {
		QueryPlan<CandidateSequence> queryPlan = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		CandidateSequence itemsUsers = candidates.get(2).merge(candidates.get(3));
		
		System.out.println(itemsUsers);
		Set<QueryPlan<CandidateSequence>> qps = queryPlan.notifyNewCandidate(itemsUsers);
		
		for(QueryPlan<CandidateSequence> qp: qps) {
			System.out.println(qp);
		}
	}
	
	@Test
	public void testQueryPlanCompaction() {
		
	}
	
	@Test
	public void testIsSubTreeOf() {
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		
		assertTrue(candidates.get(0).isSubTreeOf(usersBids));
		assertTrue(candidates.get(1).isSubTreeOf(usersBids));
		assertFalse(candidates.get(2).isSubTreeOf(usersBids));
		assertTrue(candidates.get(3).isSubTreeOf(usersBids));
	}
	
	@Test
	public void testIsSubSetOf() {
		CandidateSequence usersBids = candidates.get(0).merge(candidates.get(1));
		
		//  Candidates: User -> Bids -> Items -> User
		//                  1..n    1..5    0..3
		assertTrue(candidates.get(0).isSubSetOf(usersBids));
		assertTrue(candidates.get(1).isSubSetOf(usersBids));
		assertFalse(candidates.get(2).isSubSetOf(usersBids));
		assertTrue(candidates.get(3).isSubSetOf(usersBids));
		
	}
	
	@Test
	public void testCanMergeLargeCandidates() {
		
	}
	
	@Test
	public void testCandidateOccurrence() {
		QueryPlan<CandidateSequence> queryPlan = new QueryPlan<CandidateSequence>(usersBidsItemsSeller, candidates);
		
		assertEquals(queryPlan.candidateOccurence(candidates.get(0)), 2);
		assertEquals(queryPlan.candidateOccurence(candidates.get(1)), 1);
		assertEquals(queryPlan.candidateOccurence(candidates.get(2)), 1);
		assertEquals(queryPlan.candidateOccurence(candidates.get(3)), 2);
	}
	
	@Test
	public void testCandidateSequenceEquals() {
		
		LinkedList<CandidateSequence> candidatesTwo = new LinkedList<CandidateSequence>();
		
		for(Query query: usersBidsItemsSeller.getQueryPath()) {
			ArrayList<Query> queries = new ArrayList<Query>();
			queries.add(query);
			
			EntityTree node = new EntityTree(query.getEntity(), queries);
			CandidateSequence candidate = new CandidateSequence(node, usersBidsItemsSeller);
			candidatesTwo.add(candidate);
		}
		
		assertEquals(candidates.get(0), candidatesTwo.get(0));
		assertNotEquals(candidates.get(1), candidates.get(0));
		assertEquals(candidates, candidatesTwo);
		
		assertEquals(candidates.get(0).hashCode(), candidatesTwo.get(0).hashCode());
		assertNotEquals(candidates.get(1).hashCode(), candidates.get(0).hashCode());
		assertEquals(candidates.hashCode(), candidatesTwo.hashCode());
	}
	
	@Test
	public void testCandidateSequenceHashCode() {
		HashSet<CandidateSequence> set = new HashSet<CandidateSequence>();
		
		set.add(candidates.getLast());
		assertTrue(set.contains(candidates.getLast()));
		
		assertEquals(set.size(), 1);
		set.add(candidates.getLast());
		assertEquals(set.size(), 1);
	}
	
}
