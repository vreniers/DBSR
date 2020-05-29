package dbsr.workload.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import dbsr.config.Config;
import dbsr.cost.Cost;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.workload.Sequence;

/**
 * GENERIC: 
 * -> Table
 * -> Frequency 
 * 
 * SELECT users.id, users.name, users.lastname FROM users WHERE users.id=?
 * -> SelectedFields (users.id, ...)
 * -> ConditionalFields (users.id=?)
 * -> Frequency
 *  
 * INSERT INTO Users (username, password, fullname, email) VALUES (?, ?, ?, ?)
 * -> InsertFields (Users.username=?, ...)
 * 
 * UPDATE Users SET password=?, fullname=?, email=? WHERE username=?
 * -> UpdateFields (Users.password=?, ...)
 * -> ConditionalFields(Users.id =?, ...)
 * 
 * DELETE FROM Users WHERE username=?
 * -> ConditionalFields(Users.username=?)
 * 
 * 
 * @author vincent
 *
 */
public abstract class Query implements Cost {
	
	/**
	 * TODO: Why reference to sequences it is stored/used in? Notification in pub/subscribe? Yes.
	 */
	private final List<Sequence> sequences = new ArrayList<Sequence>();
	
	private final QueryType queryType;
	
	private final Entity table;
	
	/**
	 * Weight; how many times the query is used. 
	 * Can be initialized via the workload. => Move to workload?
	 */
	private final Integer frequency;
	
	protected Set<Field> selectFields, conditionalFields, insertFields, updateFields;

	public Query(Entity table, Integer frequency, QueryType queryType) {
		this.table = table;
		this.frequency = frequency;
		this.queryType = queryType;
		
		this.selectFields = new HashSet<Field>();
		this.updateFields = new HashSet<Field>();
		this.insertFields = new HashSet<Field>();
		this.conditionalFields = new HashSet<Field>();
	}

	public void addSequence(Sequence seq) {
		if(!this.sequences.contains(seq))
			this.sequences.add(seq);
	}
	
	public List<Sequence> getSequences() {
		return this.sequences;
	}
	
	public QueryType getQueryType() {
		return queryType;
	}
	
	public Entity getEntity() {
		return this.table;
	}

	public Integer getFrequency() {
		return frequency;
	}
	
	/**
	 * Calculate cost when only the given set of fields apply.
	 * 
	 * @param fieldsInTable
	 * @return
	 */
	public int getCost(Set<Field> fieldsInTable) {
		int cost = 0;
		
		Set<Field> remainingFields = new HashSet<Field>(getAllFields());
		remainingFields.retainAll(fieldsInTable);
		
		for(Field field: remainingFields) {
			cost += field.getCost();
		}
		
		return cost;
	}
	
	/**
	 * Returns the cost of the record, which is the number of fields manipulated,
	 * times the size of the field.
	 * 
	 * @return
	 */
	public int getCost() {
		int cost = 0;
		
		for(Field field: getAllFields()) {
			cost += field.getCost();
		}
		
		return cost;
	}
	
	public Set<Field> getAllFields() {
		HashSet<Field> fields = new HashSet<Field>();
		fields.addAll(conditionalFields);
		fields.addAll(selectFields);
		fields.addAll(insertFields);
		fields.addAll(updateFields);
		
		return fields;
	}
	
	public Set<Field> getSelectFields() {
		return selectFields;
	}

	public Set<Field> getConditionalFields() {
		return conditionalFields;
	}
	
	public Set<Field> getUpdateFields() {
		return updateFields;
	}

	public Set<Field> getInsertFields() {
		return insertFields;
	}
	
	/**
	 * Returns all affected fields from the selection statements in the query for the single table.
	 * 
	 * @return
	 */
	public Set<Field> getAffectedFields() {
		HashSet<Field> fields = new HashSet<Field>();
		
		fields.addAll(insertFields);
		fields.addAll(updateFields);
		fields.addAll(selectFields);
		fields.addAll(conditionalFields);
		
		return fields;
	}
	
	public Set<Field> getReadFields() {
		HashSet<Field> fields = new HashSet<Field>();
		
		fields.addAll(selectFields);
		fields.addAll(conditionalFields);
		
		return fields;
	}
	
	/**
	 * Can this query be answered by an EntityTree defined by otherQuery?
	 * 
	 * We get the affected fields of this query, and the fields of other query.
	 * These fields must be contained in the other fields.
	 * 
	 * @param otherQuery
	 * @return
	 */
	public boolean isAnswerableBy(Query otherQuery) {
		if(otherQuery.getEntity().equals(this.getEntity()))
			return otherQuery.getAffectedFields().containsAll(this.getAffectedFields());
		else
			return false;
	}
	
	/**
	 * If an EntityTree is defined by a set of queries (Q1,Q2,Q3),
	 * 
	 * Can this query be answered by the set?
	 * 
	 * @param queries
	 * @return
	 */
	public boolean isAnswerableBy(List<Query> queries) {
		Set<Field> fields = new HashSet<Field>();
		
		for(Query qry: queries) {
			if(qry.getEntity().equals(this.getEntity()))
				fields.addAll(qry.getReadFields());
		}
		
		return fields.containsAll(this.getReadFields());
	}

	@Override
	public String toString() {
		return "Query [" + table.getName() + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((conditionalFields == null) ? 0 : conditionalFields.hashCode());
		result = prime * result + ((frequency == null) ? 0 : frequency.hashCode());
		result = prime * result + ((insertFields == null) ? 0 : insertFields.hashCode());
		result = prime * result + ((queryType == null) ? 0 : queryType.hashCode());
		result = prime * result + ((selectFields == null) ? 0 : selectFields.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((updateFields == null) ? 0 : updateFields.hashCode());
		return result;
	}
	
	/**
	 * Ignores the sequence, and focuses on the table being selected and the fields.
	 * 
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		
		Query other = (Query) obj;
		if (conditionalFields == null) {
			if (other.conditionalFields != null)
				return false;
		} else if (!conditionalFields.equals(other.conditionalFields))
			return false;
		if (frequency == null) {
			if (other.frequency != null)
				return false;
		} else if (!frequency.equals(other.frequency))
			return false;
		if (insertFields == null) {
			if (other.insertFields != null)
				return false;
		} else if (!insertFields.equals(other.insertFields))
			return false;
		if (queryType != other.queryType)
			return false;
		if (selectFields == null) {
			if (other.selectFields != null)
				return false;
		} else if (!selectFields.equals(other.selectFields))
			return false;
		
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (updateFields == null) {
			if (other.updateFields != null)
				return false;
		} else if (!updateFields.equals(other.updateFields))
			return false;
		return true;
	}
	
	/**
	 * Checks highest number of this time query occurs in any of the sequences.
	 * 
	 * @param count
	 * @return
	 */
	public int occursMaxNrOfTimes() {
		int max = 0;
		for(Sequence seq: sequences) {
			int count = seq.countQuery(this);
			
			if(count > max)
				max = count;
		}
		
		return max;
	}
	
	
}
