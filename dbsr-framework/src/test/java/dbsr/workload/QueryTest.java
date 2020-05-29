package dbsr.workload;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import dbsr.candidate.CandidateSequence;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class QueryTest {


	private Sequence usersBidsItemsSeller;
	
	private Sequence bidsItems;
	
	private Sequence itemsBids;
	
	private Sequence itemsUsers;
	
	private Entity users, items, bids;
	
	private static Entity createEntity(String tableName) {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("users.name", "string");
		Field id = new Field("users.id", "int");
		Field date = new Field("users.date", "date");
		
		fields.add(name);
		fields.add(id);
		fields.add(date);
		
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
		
		Relationship usersPlaceBids = new Relationship("usersPlaceBids", users, bids, RelationshipType.OneToMany);
		Relationship bidsAreOnItems = new Relationship("bidsAreOnItems", bids, items, RelationshipType.ManyToOne);
		Relationship usersSellItems = new Relationship("usersSellItems", users, items, RelationshipType.OneToMany);
		
		// Set cardinalities
		usersPlaceBids.setCardinality(new Cardinality(1,3));
		bidsAreOnItems.setCardinality(new Cardinality(10,1));
		usersSellItems.setCardinality(new Cardinality(1,5));
		
		Query usersQuery = createQuery(users);
		Query bidsQuery = createQuery(bids);
		Query itemsQuery = createQuery(items);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		usersBidsItemsSeller.addQuery(createQuery(users, conditionalFields));
		
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
	}
	
	/**
	 * Tests if a query can be answered by other similar queries.
	 * 
	 */
	@Test
	public void testIsAnswerableBy() {
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		HashSet<Field> selectFields = new HashSet<Field>();
		selectFields.add(new Field("users.date", "date"));
		
		Query userAgeQry = createQuery(users, conditionalFields, selectFields);
		Query userAllQry = createQuery(users, users.getFields(), users.getFields());
		
		assertTrue(userAgeQry.isAnswerableBy(userAllQry));
		assertFalse(userAllQry.isAnswerableBy(userAgeQry));
		
		conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.name", "string"));
		Query userNameQry = createQuery(users, conditionalFields);
		
		List<Query> qryList = new ArrayList<Query>();
		qryList.add(userAgeQry);
		qryList.add(userNameQry);
		assertTrue(userAllQry.isAnswerableBy(qryList));
		
		qryList = new ArrayList<Query>();
		assertFalse(userAllQry.isAnswerableBy(qryList));
	}
}
