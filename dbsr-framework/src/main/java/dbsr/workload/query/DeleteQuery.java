package dbsr.workload.query;

import java.util.Set;

import dbsr.model.Entity;
import dbsr.model.Field;

public class DeleteQuery extends Query {

	public DeleteQuery(Entity table, Integer frequency, Set<Field> conditionalFields ) {
		super(table, frequency, QueryType.SELECT);
		
		this.conditionalFields = conditionalFields;
	}

	
	
}
