package dbsr.model;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

/**
 * Helps to create models for testing.
 * 
 * Repetively used across the unit tests.
 * 
 * @author vincent
 *
 */
public class ModelFactory {

	public static Entity createEntity(String tableName) {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field(tableName, "name");
		Field id = new Field(tableName, "id");
		
		fields.add(name);
		fields.add(id);
		
		Entity users = new Entity(tableName, id, fields);
				
		return users;
	}
	
	public static Query createQuery(Entity entity) {
		return createQuery(entity, new HashSet<Field>(entity.getFields()));
	}
	
	/**
	 * Leaves out the first field from the selection.
	 * 
	 * @param entity
	 * @return
	 */
	public static Query createQuerySubset(Entity entity) {
		HashSet<Field> fields = new HashSet<Field>(entity.getFields());
		Iterator<Field> iterator = fields.iterator();
		iterator.next();
		iterator.remove();
		
		return createQuery(entity, fields);
	}
	
	public static Query createQuery(Entity entity, Set<Field> selectFields) {
		Query query = new SelectQuery(entity, new Integer(10), selectFields, new HashSet<Field>());
		
		return query;
	}
	
	public static Query createQuery(Entity entity, Set<Field> selectFields, Set<Field> conditionalFields ) {
		Query query = new SelectQuery(entity, new Integer(10), selectFields, conditionalFields);
		
		return query;
	}
}
