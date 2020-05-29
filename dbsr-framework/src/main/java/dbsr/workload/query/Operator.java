package dbsr.workload.query;

/**
 * Query operator: currently used for * operator.
 * 
 * Query star operator fields should be filled in by the entirity of table columns before populating the model.
 * 
 * @author vincent
 */
public enum Operator {
	STAR("*");
	
	private final String operator;
	
	Operator(String op) {
		this.operator = op;
	}	
}
