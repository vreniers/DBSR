package dbsr.model;

import java.util.Set;

import dbsr.cost.Cost;

/** 
 * Description of a field for a collection, with a description of its value.
 * 
 * @author vincent
 */
public class Field implements Cost {
	
	private final String fieldName;
	
	/**
	 * TODO: Remove? We dont really use this.
	 */
	private final String value;
	
	/**
	 * Number of bytes in size. (On average)
	 */
	private final int size;
	
	public Field(String fieldName) {
		this.fieldName = fieldName;
		this.value = "";
		this.size = 1;
	}
	
	public Field(String fieldName, String value) {
		this.fieldName = fieldName;
		this.value = value;
		this.size = 1;
	}
	
	public Field(String fieldName, String value, int size) {
		this.fieldName = fieldName;
		this.value = value;
		this.size = size;
	}
	
	/**
	 * Field name without concrete value, but general size
	 * 
	 * @param fieldName
	 * @param size
	 */
	public Field(String fieldName, int size) {
		this.fieldName = fieldName;
		this.value = "";
		this.size = size;
	}
	
	/**
	 * Returns the value size (in number of bytes).
	 * 
	 * @return
	 */
	public int getCost() {
		return this.size;
	}
	
	public static int getTotalCostOfMultipleFields(Set<Field> fields) {
		int sum = 0;
		
		for(Field field: fields) {
			sum += field.getCost();
		}
		
		return sum;
	}

	public String getFieldName() {
		return fieldName;
	}

	public String getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fieldName == null) ? 0 : fieldName.hashCode());
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
		Field other = (Field) obj;
		if (fieldName == null) {
			if (other.fieldName != null)
				return false;
		} else if (!fieldName.equals(other.fieldName))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
}
