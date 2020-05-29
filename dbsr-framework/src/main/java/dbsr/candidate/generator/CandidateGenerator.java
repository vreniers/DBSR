package dbsr.candidate.generator;

import dbsr.model.Model;
import dbsr.workload.Workload;

/**
 * First generate normalized collections for each entity.
 * Based on queries suggest new collections.
 * 
 * 1) Establish query -> model links 
 * 	=> Done: SequenceGraph (per start entity?).
 * 2) Next: collapse and generate candidates for parts of the Root -> Leaf paths.
 * 	=> Create variants of each path on the queries affecting each node: Attribute selectivity.
 * 3) Determine cost for a path based on cardinalities...
 * 
 * @author vincent
 *
 */
public class CandidateGenerator {
	
	private final Workload workload;
	
	private final Model model;
	
	public CandidateGenerator(Workload workload, Model model) {
		this.workload = workload;
		this.model = model;
	}
}
