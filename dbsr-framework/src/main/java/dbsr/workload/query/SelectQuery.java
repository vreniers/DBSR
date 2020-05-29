package dbsr.workload.query;

import java.util.Set;

import dbsr.config.Config;
import dbsr.model.Entity;
import dbsr.model.Field;

public class SelectQuery extends Query {

	public SelectQuery(Entity table, Integer frequency, Set<Field> selectFields, Set<Field> conditionalFields ) {
		super(table, frequency, QueryType.SELECT);
		
		/**
		 * If vertical slicing isn't allowed we override the selection of fields with that of the entire table's fields.
		 */
		if(Config.VERTICAL_SLICING) {
			this.selectFields = selectFields;
		} else {
			this.selectFields = table.getFields();
		}
		
		this.conditionalFields = conditionalFields;
	}

	

}
