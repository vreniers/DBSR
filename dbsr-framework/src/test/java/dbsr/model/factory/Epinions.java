package dbsr.model.factory;

import java.util.HashSet;
import java.util.Set;

import dbsr.model.Entity;
import dbsr.model.Field;

public class Epinions {
	
	private final Entity users, items, reviews;
	
	public Epinions() {
		users = createUsers();
		items = createItems();
		reviews = createReviews();
		
		createRelationships();
	}
	
	/**
	 * Model relationships
	 */
	private void createRelationships() {
		
	}
	
	
	/**
	 * Model entities
	 */
	public static Entity createUsers() {
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
		Field id = new Field("id", 15);
		
		fields.add(name);
		fields.add(id);
		fields.add(price);
		
		Entity items = new Entity("items", id, fields);
		
		return items;
	}
	
	private Entity createReviews() {
		Set<Field> fields = new HashSet<Field>();
		Field rating = new Field("ratingScore", 5);
		Field reviewDescription = new Field("reviewDescription", 100);
		Field date = new Field("date", 30);
		Field id = new Field("id", 15);
		
		fields.add(rating);
		fields.add(id);
		fields.add(reviewDescription);
		fields.add(date);
		
		Entity reviews = new Entity("reviews", id, fields);
		
		return reviews;
	}
	
}
