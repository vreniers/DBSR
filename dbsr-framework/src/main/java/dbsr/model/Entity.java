package dbsr.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dbsr.model.relationship.Relationship;

public class Entity {
	
	private final String name;
	
	private final Field primary_key;
	
	private final Set<Field> fields;
	
	private final List<Relationship> relationships;
	
	public Entity(String name, Field pk, Set<Field> fields) {
		this(name,pk,fields, new ArrayList<Relationship>());
	}
	
	public Entity(String name, Field pk, Set<Field> fields, List<Relationship> relationships) {
		this.name = name;
		this.primary_key = pk;
		this.fields = fields;
		this.relationships = relationships;
	}

	public String getName() {
		return name;
	}

	public Field getPrimary_key() {
		return primary_key;
	}

	public Set<Field> getFields() {
		return fields;
	}
	
	/**
	 * Checks whether the given entity has a relationship with this entity.
	 * 
	 * @param entity
	 * @return
	 */
	public boolean hasRelationshipWith(Entity entity) {
		Relationship relation = getRelationshipWith(entity);
		
		return relation != null;
	}
	
	/**
	 * Returns the relationship corresponding to the given entity.
	 * 
	 * @param entity
	 * @return
	 */
	public Relationship getRelationshipWith(Entity entity) {
		for(Relationship relation: relationships) {
			if(relation.getSource().equals(entity) || relation.getTarget().equals(entity)) {
				return relation;
			}
		}
		
		return null;
	}
	
	public void addRelationship(Relationship relation) {
		if(!this.relationships.contains(relation))
			this.relationships.add(relation);
	}
	
	public List<Relationship> getRelationships() {
		return this.relationships;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((primary_key == null) ? 0 : primary_key.hashCode());
		result = prime * result + ((relationships == null) ? 0 : relationships.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity other = (Entity) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (primary_key == null) {
			if (other.primary_key != null)
				return false;
		} else if (!primary_key.equals(other.primary_key))
			return false;
		if (relationships == null) {
			if (other.relationships != null)
				return false;
		} else if (!relationships.equals(other.relationships))
			return false;
		return true;
	}
	
	public boolean equalsWithoutRel(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Entity other = (Entity) obj;
		if (fields == null) {
			if (other.fields != null)
				return false;
		} else if (!fields.equals(other.fields))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (primary_key == null) {
			if (other.primary_key != null)
				return false;
		} else if (!primary_key.equals(other.primary_key))
			return false;
		
		return true;
	}

	@Override
	public String toString() {
		return "Entity [name=" + name + ", primary_key=" + primary_key + ", fields=" + fields + ", relationships="
				+ relationships + "]";
	}

}
