package dbsr.candidates.generator;

import static org.junit.Assert.fail;

import java.util.HashSet;

import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbsr.candidate.CandidateSequences;
import dbsr.candidate.generator.MultiSequenceCandidateGenerator;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.factory.Ebay;
import dbsr.model.factory.RUBiS;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class MultiSequenceCandidateGeneratorTest {
	
	private Set<Sequence> sequences;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		Ebay ebay = new Ebay();
		RUBiS rubis = new RUBiS();
		
		sequences = rubis.getSequences();
	}
	
	/**
	 * items two query EntityTree shouldn't be answerable for ItemsQuery.
	 * 
	 * Not sure why 0 0 0 X 0 0 but reverse not true in output.
	 * 
	 * TODO: Sanity check
	 */
	@Test
	public void testItemsTwoNotAnswerableforItemsOne() {
		// fail("to be implemented");
	}
	
	/**
	 * 
	 */
	@Test
	public void testCandidateGenerator() {
		MultiSequenceCandidateGenerator generator = new MultiSequenceCandidateGenerator(sequences);
		
		try {
			generator.startGeneration();
		} catch (Exception exc) { 
			System.out.println("catch this");
			System.out.println(exc.getMessage());
			System.out.println(exc);
		}
		for(CandidateSequences candidate: generator.getCandidates()) {
			System.out.println(candidate);
		}
		
		generator.printQueryPlansPerSequence();
		generator.printCandidates();
	}
	
	@Test
	public void testTreeSetIterator() {
		// Ordering goes in Size when iterating.
		
		// Test compaction.
		
	}
	
//	@Test
//	public void testSingleSequenceGenerator() {
//		SequenceCandidateGenerator generator = new SequenceCandidateGenerator(itemsBids);
//		
//		generator.startGeneration();
//		
//		for(CandidateSequence candidate: generator.getCandidates()) {
//			System.out.println(candidate);
//		}
//		
//		for(QueryPlan<CandidateSequence> qp: generator.getExistingQueryPlans()) {
//			System.out.println(qp);
//		}
//		
//	}
}
