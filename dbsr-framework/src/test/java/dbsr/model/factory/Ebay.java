package dbsr.model.factory;

import java.util.HashSet;
import java.util.Set;

import dbsr.candidate.CandidateSequences;
import dbsr.candidate.generator.MultiSequenceCandidateGenerator;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.ModelFactory;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class Ebay {

	private Sequence usersBidsItemsSeller;
	
	private Sequence usersRegions;
	
	private Sequence bidsUsers;
	
	private Sequence bidsItems;
	
	private Sequence itemsBids;
	
	private Sequence itemsUsers;
	
	private Sequence usersItems;
	
	private Set<Sequence> sequences;
	
	private Entity users, items, bids, regions;
	
	public Ebay() {
		createEntities();
		
		createRelationships();
		
		createSequencesSameQueries();
	}
	

	/**
	 * Model workload 
	 */
	private void createSequencesNormalized() {
		Query usersQuery = ModelFactory.createQuery(users);
		Query bidsQuery = ModelFactory.createQuery(bids);
		Query itemsQuery = ModelFactory.createQuery(items);
		Query regionsQuery = ModelFactory.createQuery(regions);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller = new Sequence();
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersQuery);
		
//		// --- ItemsBids ---
		itemsBids = new Sequence();
		itemsBids.addQuery(itemsQuery);
		itemsBids.addQuery(bidsQuery);
//		
//		
//		// --- BidsItems ---
		bidsItems = new Sequence();
		bidsItems.addQuery(bidsQuery);
//		
//		HashSet<Field> conditionalFields = new HashSet<Field>();
//		HashSet<Field> selectFields = new HashSet<Field>();
//		selectFields.add(new Field("type"));
		bidsItems.addQuery(itemsQuery);
//		
		// --- ItemsUsers ---
		itemsUsers = new Sequence();
		itemsUsers.addQuery(itemsQuery);
		itemsUsers.addQuery(usersQuery);
//		
		usersItems = new Sequence();
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(itemsQuery);
//		
		bidsUsers = new Sequence();
		bidsUsers.addQuery(bidsQuery);
		bidsUsers.addQuery(usersQuery);
		
		usersRegions = new Sequence();
		usersRegions.addQuery(usersQuery);
		usersRegions.addQuery(regionsQuery);
		
		sequences = new HashSet<Sequence>();
		sequences.add(usersBidsItemsSeller);
		sequences.add(usersRegions);
		sequences.add(bidsUsers);
		sequences.add(bidsItems);
		sequences.add(itemsBids);
		sequences.add(itemsUsers);
		sequences.add(usersItems);
	}
	
	private void createSequencesSameQueries() {
		Query usersQuery = ModelFactory.createQuery(users);
		Query bidsQuery = ModelFactory.createQuery(bids);
		Query itemsQuery = ModelFactory.createQuery(items);
		Query regionsQuery = ModelFactory.createQuery(regions);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller = new Sequence();
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersQuery);
		
		usersRegions = new Sequence();
		usersRegions.addQuery(usersQuery);
		usersRegions.addQuery(regionsQuery);
		
		sequences = new HashSet<Sequence>();
		sequences.add(usersBidsItemsSeller);
		sequences.add(usersRegions);
	}
	
	private void createSequencesOverlappingQueries() {
		Query usersQuery = ModelFactory.createQuery(users);
		Query bidsQuery = ModelFactory.createQuery(bids);
		Query itemsQuery = ModelFactory.createQuery(items);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller = new Sequence();
		usersBidsItemsSeller.addQuery(ModelFactory.createQuerySubset(users));
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersQuery);
		
		
		usersItems = new Sequence();
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(ModelFactory.createQuerySubset(items));
		
		sequences = new HashSet<Sequence>();
		sequences.add(usersBidsItemsSeller);
		sequences.add(usersItems);
	}
	
	public Set<Sequence> getSequences() {
		return new HashSet<Sequence>(this.sequences);
	}
	
	/**
	 * Model relationships
	 */
	private void createRelationships() {
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
	}
	
	/**
	 * Model entities
	 */
	private void createEntities() {
		users = createUsers();
		items = createItems();
		bids = createBids();
		regions = createRegions();
	}
	
	private Entity createUsers() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("firstName", 20);
		Field lastName = new Field("lastName", 25);
		Field id = new Field("id", 15);
		fields.add(name);
		fields.add(id);
		fields.add(lastName);
		
		Entity users = new Entity("users", id, fields);
		
		return users;
	}
	
	private Entity createItems() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("productTitle", 20);
		Field price = new Field("price", 5);
		Field type = new Field("type", 5);
		Field date = new Field("date", 20);
		Field id = new Field("id", 15);
		
		fields.add(type);
		fields.add(name);
		fields.add(id);
		fields.add(price);
		fields.add(date);
		
		Entity items = new Entity("items", id, fields);
		
		return items;
	}
	
	private Entity createBids() {
		Set<Field> fields = new HashSet<Field>();
		Field amount = new Field("amount", 5);
		Field date = new Field("date", 30);
		Field id = new Field("id", 15);
		
		fields.add(amount);
		fields.add(id);
		fields.add(date);
		
		Entity bids = new Entity("bids", id, fields);
		
		return bids;
	}
	
	private Entity createRegions() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("region", 10);
		Field id = new Field("id", 15);
		
		fields.add(name);
		fields.add(id);
		
		Entity regions = new Entity("regions", id, fields);
		
		return regions;
	}
}
