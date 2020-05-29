package dbsr.model;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class EntityTest {
	
	private Entity users, bids, items;

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
	}
	

	@Test
	public void testEqual() {
		Entity entityOne = createEntity("test");
		Entity entityTwo = createEntity("test");
		
		assertEquals(entityOne, entityTwo);
	}
	
	@Test
	public void testEqualsRelationShips() {
		Entity entityOne = createEntity("test");
		Entity entityTwo = createEntity("test");
		
		Entity entityThree = createEntity("other");
		
		Relationship rel = new Relationship("relation", entityOne, entityThree);
		
		assertNotEquals(entityOne, entityTwo);
		
		Relationship relTwo = new Relationship("relation", entityTwo, entityThree);
		
		assertEquals(rel, relTwo);
		assertEquals(entityOne, entityTwo);
	}
	
	@Test
	public void testEqualsHashCode() {
		Entity entityOne = createEntity("test");
		Entity entityTwo = createEntity("test");
		
		assertEquals(entityOne.hashCode(), entityTwo.hashCode());
	}
	
	@Test
	public void testEqualsHashCodeRelationShips() {
		Entity entityOne = createEntity("test");
		Entity entityTwo = createEntity("test");
		
		Entity entityThree = createEntity("other");
		
		Relationship rel = new Relationship("relation", entityOne, entityThree);
		
		assertNotEquals(entityOne.hashCode(), entityTwo.hashCode());
		
		Relationship relTwo = new Relationship("relation", entityTwo, entityThree);
		
		assertEquals(rel.hashCode(), relTwo.hashCode());
		assertEquals(entityOne.hashCode(), entityTwo.hashCode());
		assertNotEquals(entityOne.hashCode(), entityThree.hashCode());
	}

}
