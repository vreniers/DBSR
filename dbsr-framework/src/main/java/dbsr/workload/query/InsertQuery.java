package dbsr.workload.query;

import java.util.Set;

import dbsr.model.Entity;
import dbsr.model.Field;

public class InsertQuery extends Query {

	public InsertQuery(Entity table, Integer frequency, Set<Field> updateFields, Set<Field> conditionalFields ) {
		super(table, frequency, QueryType.SELECT);
		
		this.updateFields = updateFields;
		this.conditionalFields = conditionalFields;
	}

	

}
