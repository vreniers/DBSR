package dbsr.model.relationship;

import dbsr.model.Entity;

/**
 * Type of relationship (1-1, 1-n, m-n) between 2 entities.
 * 
 * @author vincent
 */
public class Relationship {
	
	public enum RelationshipType {
		OneToOne,
		OneToMany,
		ManyToMany,
		ManyToOne,
	}
	
	private final boolean bidirectional;

	private final Entity source;
	
	private final Entity target;
	
	private final String name;
	
	private Cardinality cardinality;
	
	private final RelationshipType type;
	
	public Relationship(String name, Entity source, Entity target) {
		this(name, source, target, RelationshipType.OneToOne);
	}
	
	public Relationship(String name, Entity source, Entity target, RelationshipType type, boolean bidirectional) {
		this.name = name;
		this.source = source;
		this.target = target;
		this.bidirectional = bidirectional;
		this.type = type;
		
		source.addRelationship(this);
		target.addRelationship(this);
	}
	
	public Relationship(String name, Entity source, Entity target, RelationshipType type) {
		this(name, source, target, type, false);
	}
	
	/**
	 * Return the cardinality for a given entity.
	 * 
	 * @param entity
	 * @return
	 */
	public int getCardinality(Entity entity) {
		if(entity.equals(source))
			return this.getCardinality().getSourceFreq();
		
		if(entity.equals(target))
			return this.getCardinality().getTargetFreq();
		
		throw new IllegalArgumentException("Invalid entity given for relationship cardinality.");
	}
	
	/**
	 * Checks if the relationships is always present.
	 * Cardinality has to be at least 1 at the source.
	 * 
	 * @return
	 */
	public boolean alwaysExistsAtSource() {
		return getCardinality().getSourceFreq() >= 1;
	}
	
	/**
	 * Returns the size which the given entity targets the other entity.
	 * E.g. User - Comments is 1:n, given User, the result is n.
	 * E.g. User - Regions is 1000:1, given Region the result is 1000.
	 * 
	 * @param endEntity
	 * @return
	 */
	public int getCardinalityEntityTargeting(Entity entity) {
		if(entity.equals(source))
			return getCardinality(target);
		if(entity.equals(target))
			return getCardinality(source);
		
		throw new IllegalArgumentException("Invalid entity given for relationship cardinality.");
	}
	
	public String getName() {
		return this.name;
	}
	
	/**
	 * Check whether type matches cardinality, or infer type from cardinality.
	 * 
	 * @param cardinality
	 * @param type
	 */
	public void setCardinality(Cardinality cardinality) {
		this.cardinality = cardinality;
	}
	
	public boolean isBidirectional() {
		return bidirectional;
	}

	public Entity getSource() {
		return source;
	}

	public Entity getTarget() {
		return target;
	}

	public Cardinality getCardinality() {
		return cardinality;
	}

	public RelationshipType getType() {
		return type;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (bidirectional ? 1231 : 1237);
		result = prime * result + ((cardinality == null) ? 0 : cardinality.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((source == null) ? 0 : source.getName().hashCode());
		result = prime * result + ((target == null) ? 0 : target.getName().hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		Relationship other = (Relationship) obj;
		if (bidirectional != other.bidirectional)
			return false;
		if (cardinality == null) {
			if (other.cardinality != null)
				return false;
		} else if (!cardinality.equals(other.cardinality))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (source == null) {
			if (other.source != null)
				return false;
		} else if (!source.equalsWithoutRel(other.source))
			return false;
		if (target == null) {
			if (other.target != null)
				return false;
		} else if (!target.equalsWithoutRel(other.target))
			return false;
		if (type != other.type)
			return false;
		return true;
	}
	
	
	
}
