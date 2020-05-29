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

public class RUBiS {

	private Sequence usersBidsItemsSeller, usersBidsItems;
	
	private Sequence usersRegions;
	
	private Sequence bidsUsers;
	
	private Sequence bidsItems;
	
	private Sequence itemsBids;
	
	private Sequence itemsUsers;
	
	private Sequence usersItems;
	
	private Sequence itemsComments, usersComments, itemsCommentsUsers;
	
	//TODO: Remove? pretty heavy perhaps
	private Sequence regionsUsers;
	
	private Set<Sequence> sequences;
	
	private Entity users, items, bids, regions, comments;
	
	public RUBiS() {
		createEntities();
		
		createRelationships();
		
		createSequencesNormalized();
	}
	

	/**
	 * Model workload 
	 */
	private void createSequencesNormalized() {
		Query usersQuery = ModelFactory.createQuery(users);
		Query bidsQuery = ModelFactory.createQuery(bids);
		Query itemsQuery = ModelFactory.createQuery(items);
		Query regionsQuery = ModelFactory.createQuery(regions);
		Query commentsQuery = ModelFactory.createQuery(comments);
		
		// --- usersBidsItemsSeller -----
		usersBidsItemsSeller = new Sequence(15);
		usersBidsItemsSeller.addQuery(usersQuery);
		usersBidsItemsSeller.addQuery(bidsQuery);
		usersBidsItemsSeller.addQuery(itemsQuery);
		usersBidsItemsSeller.addQuery(usersQuery);
		
		// ---- usersBidsItems ----
		usersBidsItems = new Sequence(50);
		usersBidsItems.addQuery(usersQuery);
		usersBidsItems.addQuery(bidsQuery);
		usersBidsItems.addQuery(itemsQuery);
		
//		// --- ItemsBids ---
		itemsBids = new Sequence(50);
		itemsBids.addQuery(itemsQuery);
		itemsBids.addQuery(bidsQuery);
//		
//		
//		// --- BidsItems ---
		bidsItems = new Sequence(5);
		bidsItems.addQuery(bidsQuery);
		bidsItems.addQuery(itemsQuery);
//		
		// --- ItemsUsers ---
		itemsUsers = new Sequence(15);
		itemsUsers.addQuery(itemsQuery);
		itemsUsers.addQuery(usersQuery);
//		
		// ---- UsersItems -----
		usersItems = new Sequence(15);
		usersItems.addQuery(usersQuery);
		usersItems.addQuery(itemsQuery);
		
		// --- UsersComments ----
		usersComments = new Sequence(10);
		usersComments.addQuery(usersQuery);
		usersComments.addQuery(commentsQuery);
		
		// --- ItemsComments ----
		itemsComments = new Sequence(50);
		itemsComments.addQuery(itemsQuery);
		itemsComments.addQuery(commentsQuery);
		
		// --- ItemsCommentUsers ----
		itemsCommentsUsers = new Sequence(20);
		itemsCommentsUsers.addQuery(itemsQuery);
		itemsCommentsUsers.addQuery(commentsQuery);
		itemsCommentsUsers.addQuery(usersQuery);
		
		//	--- BidsUsers ----
		bidsUsers = new Sequence(10);
		bidsUsers.addQuery(bidsQuery);
		bidsUsers.addQuery(usersQuery);
		
		// --- UsersRegions ---
		usersRegions = new Sequence(5);
		usersRegions.addQuery(usersQuery);
		usersRegions.addQuery(regionsQuery);
		
		// --- RegionsUsers ----
		regionsUsers = new Sequence();
		regionsUsers.addQuery(regionsQuery);
		regionsUsers.addQuery(usersQuery);
		
		sequences = new HashSet<Sequence>();
		sequences.add(usersBidsItemsSeller);
		sequences.add(usersBidsItems);
		sequences.add(usersRegions);
		sequences.add(bidsUsers);
		sequences.add(bidsItems);
		sequences.add(itemsBids);
		sequences.add(itemsUsers);
		sequences.add(usersItems);
		sequences.add(itemsComments);
		sequences.add(usersComments);
		sequences.add(itemsCommentsUsers);
//		sequences.add(regionsUsers);
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
		// Relationship itemsHaveSeller = new Relationship("itemHasSeller", items, users, RelationshipType.ManyToOne); //EQUI
		
		Relationship usersHaveRegion = new Relationship("usersHasRegion", users, regions, RelationshipType.ManyToOne);
		
		Relationship itemsHaveComments = new Relationship("itemsHaveComments", items, comments, RelationshipType.OneToMany);
		Relationship usersPlaceComments = new Relationship("usersPlaceComments", users, comments, RelationshipType.OneToMany);
		
		// Set cardinalities
		usersPlaceBids.setCardinality(new Cardinality(1,15));
		bidsAreOnItems.setCardinality(new Cardinality(3,1)); // RATIO
		usersSellItems.setCardinality(new Cardinality(1,5));
		usersHaveRegion.setCardinality(new Cardinality(5000,1));
		
		itemsHaveComments.setCardinality(new Cardinality(1, 2)); // RATIO
		usersPlaceComments.setCardinality(new Cardinality(1,10));
	}
	
	/**
	 * Model entities
	 */
	private void createEntities() {
		users = createUsers();
		items = createItems();
		bids = createBids();
		regions = createRegions();
		comments = createComments();
	}
	
	private Entity createUsers() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("firstName", 20);
		Field lastName = new Field("lastName", 25);
		Field about = new Field("about", 70);
		Field id = new Field("id", 15);
		
		fields.add(name);
		fields.add(id);
		fields.add(lastName);
		fields.add(about);
		
		Entity users = new Entity("users", id, fields);
		
		return users;
	}
	
	private Entity createComments() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("commentTitle", 20);
		Field commentText = new Field("commentText", 70);
		Field date = new Field("date", 20);
		Field id = new Field("id", 15);
		
		fields.add(name);
		fields.add(id);
		fields.add(commentText);
		fields.add(date);
		
		Entity comments = new Entity("comments", id, fields);
		
		return comments;
	}
	
	private Entity createItems() {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field("productTitle", 20);
		Field price = new Field("price", 5);
		Field type = new Field("type", 5);
		Field date = new Field("date", 20);
		Field id = new Field("id", 15);
		Field description = new Field("description", 80);
		
		fields.add(type);
		fields.add(name);
		fields.add(id);
		fields.add(price);
		fields.add(date);
		fields.add(description);
		
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
