package dbsr.workload.query;

import java.util.Set;

import dbsr.model.Entity;
import dbsr.model.Field;

public class UpdateQuery extends Query {

	public UpdateQuery(Entity table, Integer frequency, Set<Field> insertFields ) {
		super(table, frequency, QueryType.SELECT);
		
		this.insertFields = insertFields;
	}

	

}
