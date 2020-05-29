package dbsr.config;

public class Config {
	
	public static final boolean MULTI_THREADING_NOTIFIER = true;
	
	/**
	 * Number of threads to process notification of new data structure.
	 */
	public static final int MAX_THREADS = 3;
	
	/**
	 * Can the document store cut-off results from a selected documented at a certain depth.
	 * TODO
	 */
	public static final boolean PROJECTION_DEPTH = false;
	
	/**
	 * MAX_DOCUMENT_DEPTH
	 */
	public static final int MAX_DOCUMENT_DEPTH = 3;
	
	/**
	 * MAX_DOCUMENT_WIDTH
	 */
	public static final int MAX_DOCUMENT_WIDTH = 1;
	
	/**
	 * TABLE VERTICAL SLICING
	 * Setting this to true allows document suggestions with a partial view of the original normalized table.
	 * (E.g. subset of the original columns).
	 * Setting this to false only allows for combinations of the original tables.
	 * 
	 * TODO: Is probably best facilitated via correct INPUT, use same query objects across sequences.
	 * Otherwise generator has to link similar query objects (merge), but not yet implemented...
	 */
	public static final boolean VERTICAL_SLICING = true;
	
	/**
	 * Prune each sequence's bucket and leave the X best query plans.
	 */
	public static final int PRUNE_LEAVE_NR_PLANS_PER_BUCKET = 500;
	
	public static final int MAX_NR_DOCUMENTS = 2;
	
	/**
	 * This is the maximum number of embedded childs regardless of field size.
	 */
	public static final int MAX_SINGLE_DOCUMENT_EMBEDDED_AMOUNT = 350; 
	
	/**
	 * Prune when the remaining stack size reaches X query plans.
	 */
	public static final int PRUNE_AT_QUERY_STACK_SIZE = 30000;
	
	/**
	 * Maximum iterations for the generator.
	 */
	public static final int MAX_ITERATIONS = 4000;
}
