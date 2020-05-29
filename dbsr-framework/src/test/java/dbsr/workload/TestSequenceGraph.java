package dbsr.workload;

import static org.junit.Assert.*;


import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.model.tree.EntityTree;
import dbsr.workload.Sequence;
import dbsr.workload.SequenceGraph;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class TestSequenceGraph {
	
private Entity users, items, bids;
	
	private Sequence usersBidsItemsSeller, usersItems, usersRegions;
	
	private SequenceGraph graph;
	
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
//		createEntity();
//		createQueries();
//		createSequence();
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
	
	@Before
	public void setUp(){
		
	}
	
	/**
	 * Creates sequence with Users and Regions and query on both tables.
	 * 
	 * @return
	 */
	@Before
	public void createSingleSequence() {
		usersRegions = new Sequence();
		
		Entity users = createEntity("users");
		Entity region = createEntity("regions");
		
		new Relationship("usersHaveRegion", users, region, RelationshipType.ManyToOne);
		
		Query queryUsers = createQuery(users);
		Query queryRegion = createQuery(region);
		
		usersRegions.addQuery(queryUsers);
		usersRegions.addQuery(queryRegion);;
	}
	
	/**
	 * Creates sequence with Users and Regions and query on both tables.
	 * 
	 * @return
	 */
	@Before
	public void createSequenceGraph() {
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
		
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		usersBidsItemsSeller.addQuery(createQuery(users, conditionalFields));
		
		graph = new SequenceGraph(usersBidsItemsSeller);
		
		usersItems = new Sequence();
		usersItems.addQuery(createQuery(users));
		usersItems.addQuery(createQuery(items));
		
		graph.addSequence(usersItems);
	}
	

	@Test
	public void testTreeSingleSequence() {
		SequenceGraph graph = new SequenceGraph(usersRegions);
		
		assertEquals(graph.getGraph().getNode().getName(), "users");
		assertEquals(graph.getGraph().getChildren().get(0).getNode().getName(), "regions");
	}
	
	@Test
	public void testSequenceGraphCreation() {
		assertEquals(graph.getGraph().getNode().getName(), "users");
		assertEquals(graph.getGraph().getChildren().get(0).getNode().getName(), "bids");
		assertEquals(graph.getGraph().getChildren().get(1).getNode().getName(), "items");
		
		EntityTree bids = graph.getGraph().getChildren().get(0);
		assertEquals(bids.getChildren().get(0).getNode().getName(), "items");
		assertEquals(bids.getChildren().get(0).getChildren().get(0).getNode().getName(), "users");
	}

}
