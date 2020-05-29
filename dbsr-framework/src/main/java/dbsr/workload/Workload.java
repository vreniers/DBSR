package dbsr.workload;

import java.util.ArrayList;
import java.util.List;

/**
 * Workload is comprised of sequences of queries.
 * Sequence can be a stand-alone query.
 * 
 * Each sequence impacts a set of tables and columns.
 * 
 * Inital step: only associate query with entire table.
 * Improved: associated query with parts of tables.
 * 
 * @author vincent
 */
public class Workload {
	
	private final String name;
	
	private final List<Sequence> sequences;
	
	public Workload(String workloadName) {
		this(workloadName, new ArrayList<Sequence>());
	}
	
	public Workload(String workloadName, List<Sequence> sequences) {
		this.name = workloadName;
		this.sequences = sequences;
	}
	
	public void addSequence(Sequence seq) {
		this.sequences.add(seq);
	}
	
	public List<Sequence> getWorkload() {
		return this.sequences;
	}
	
	//TODO: Get impacted table + impacted fields per sequence.
}
