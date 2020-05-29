package dbsr.model;

import java.util.HashMap;

/**
 * - Entities
 * 
 * - Relationships
 * 
 * @author vincent
 *
 */
public abstract class Model {
	
	private final HashMap<String, Entity> entities;
	
	public Model() {
		this.entities = new HashMap<String, Entity>();
	}
	
	/**
	 * Add verification for relationships on existing entities.
	 * 
	 * @param entities
	 * @param relationships
	 */
	public Model(HashMap<String, Entity> entities) {
		this.entities = entities;
	}
	
	public void addEntity(Entity entity) {
		if(entity == null)
			throw new IllegalArgumentException("Wrong entity information.");
		
		if(!entities.containsKey(entity.getName()) && !entities.containsValue(entity))
			entities.put(entity.getName(), entity);
	}
	
	/**
	 * Don't add double entities.
	 * 
	 * @param tableName
	 * @param entity
	 */
	public void addEntity(String tableName, Entity entity) {
		if(entity == null || tableName == null)
			throw new IllegalArgumentException("Wrong entity information.");
		
		if(entity.getName() != tableName)
			throw new IllegalArgumentException("Table name does not match entity table name.");
		
		if(!entities.containsKey(tableName) && !entities.containsValue(entity))
			entities.put(tableName, entity);
	}
	
	
	public Entity getEntity(String tableName) {
		return entities.get(tableName);
	}
	
	public HashMap<String,Entity> getEntities() {
		return entities;
	}
}
