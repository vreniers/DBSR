package dbsr.cost;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import dbsr.candidate.CandidateSequences;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.ModelFactory;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.model.tree.EntityTree;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class CostCalculatorEntityTreeTest {
	
	private Sequence usersBidsItemsSeller;
	
	private CandidateSequences usersBidsItemsUsersCandidate;
	
	private Entity users, items, bids, regions;
	
	private Query usersQuery, bidsQuery, itemsQuery, usersTwoQuery;
	
	private EntityTree usersBidsItemsSellerTree;
	
	@Before
	public void setup() {
		createSequences();
		
		createCandidates();
	}
	
	private void createCandidates() {
		usersBidsItemsSellerTree = EntityTree.createEntityTree(usersBidsItemsSeller.getQueryPath());
		
		usersBidsItemsUsersCandidate = new CandidateSequences(usersBidsItemsSellerTree);
	}
	
	private void createSequences() {
		usersBidsItemsSeller = new Sequence();
		
		users = ModelFactory.createEntity("users");
		items = ModelFactory.createEntity("items");
		bids = ModelFactory.createEntity("bids");
		regions = ModelFactory.createEntity("regions");
		
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
		
		usersQuery = ModelFactory.createQuery(users);
		bidsQuery = ModelFactory.createQuery(bids);
		itemsQuery = ModelFactory.createQuery(items);		
		usersTwoQuery = ModelFactory.createQuery(users, users.getFields(), conditionalFields);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersTwoQuery);
	}
	
	@Test
	public void testCostOfSmallerVsBiggerTrees() {
		EntityTree bigTree = this.usersBidsItemsSellerTree;
		EntityTree bidsItemsSellerTree = bigTree.getChildren().get(0).clone();
		EntityTree itemsSellerTree = bidsItemsSellerTree.getChildren().get(0).clone();
		EntityTree sellerTree = itemsSellerTree.getChildren().get(0).clone();
		
		assertTrue(bigTree.getCost() > bidsItemsSellerTree.getCost());
		assertTrue(bidsItemsSellerTree.getCost() > itemsSellerTree.getCost());
		assertTrue(itemsSellerTree.getCost() > sellerTree.getCost());
		
//		System.out.println(usersBidsItemsSellerTree.getCost());
//		System.out.println(usersBidsItemsSellerTree.getChildren().get(0).getCost());
		
		// Assure that clones have equal cost. (Technical check)
		assertEquals(usersBidsItemsSellerTree.getChildren().get(0).clone().getCost(),
				usersBidsItemsSellerTree.getChildren().get(0).getCost());
	}
	
	@Test
	public void testCostCardinality() {
		EntityTree bigTree = this.usersBidsItemsSellerTree;
		EntityTree bidsItemsSellerTree = bigTree.getChildren().get(0).clone();
		EntityTree itemsSellerTree = bidsItemsSellerTree.getChildren().get(0).clone();
		EntityTree sellerTree = itemsSellerTree.getChildren().get(0).clone();

		// cardinality between bids and items is 10 - 1; so its a 1-1-1 type of structure.
		// cost is 2 extra fields on top of items seller tree.
		assertEquals(bidsItemsSellerTree.getCost() - 2,  itemsSellerTree.getCost());
	}
	
	/**
	 * Tests a singular node with many fields.
	 * 
	 */
	@Test
	public void testCostBigNode() {
		
	}
}
