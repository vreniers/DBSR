package dbsr.candidates.generator;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import dbsr.candidate.CandidateSequence;
import dbsr.candidate.generator.SequenceCandidateGenerator;
import dbsr.model.Entity;
import dbsr.model.Field;
import dbsr.model.relationship.Cardinality;
import dbsr.model.relationship.Relationship;
import dbsr.model.relationship.Relationship.RelationshipType;
import dbsr.workload.QueryPlan;
import dbsr.workload.Sequence;
import dbsr.workload.query.Query;
import dbsr.workload.query.SelectQuery;

public class SequenceCandidateGeneratorTest {

	private Sequence usersBidsItemsSeller;
	
	private Entity users, items, bids;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		usersBidsItemsSeller = new Sequence();
		
		users = createEntity("users");
		items = createEntity("items");
		bids = createEntity("bids");
		
		Relationship usersPlaceBids = new Relationship("usersPlaceBids", users, bids, RelationshipType.OneToMany);
		Relationship bidsAreOnItems = new Relationship("bidsAreOnItems", bids, items, RelationshipType.ManyToOne);
		Relationship usersSellItems = new Relationship("usersSellItems", users, items, RelationshipType.OneToMany);
		//Relationship itemsHaveSeller = new Relationship("itemHasSeller", items, users, RelationshipType.ManyToOne); //EQUI
		
		// Set cardinalities
		usersPlaceBids.setCardinality(new Cardinality(1,3));
		bidsAreOnItems.setCardinality(new Cardinality(10,1));
		usersSellItems.setCardinality(new Cardinality(1,5));
		
		usersBidsItemsSeller.addQuery(createQuery(users));
		usersBidsItemsSeller.addQuery(createQuery(bids));
		usersBidsItemsSeller.addQuery(createQuery(items));
		
		HashSet<Field> conditionalFields = new HashSet<Field>();
		conditionalFields.add(new Field("users.id", "int"));
		usersBidsItemsSeller.addQuery(createQuery(users, conditionalFields));
	}
	
	private static Entity createEntity(String tableName) {
		Set<Field> fields = new HashSet<Field>();
		Field name = new Field(tableName, "name");
		Field id = new Field(tableName, "id");
		
		fields.add(name);
		fields.add(id);
		
		Entity users = new Entity(tableName, id, fields);
				
		return users;
	}
	
	private static Query createQuery(Entity entity) {
		return createQuery(entity, new HashSet<Field>());
	}
	
	private static Query createQuery(Entity entity, Set<Field> conditionalFields) {
		Query query = new SelectQuery(entity, new Integer(10), entity.getFields(), conditionalFields);
		
		return query;
	}

	/**
	 * Current output:
	 *  [ users ]
		[ bids ]
		[ items ]
		[ users ]
		[ users [ bids ] ]
		[ bids [ items ] ]
		[ items [ users ] ]
		[ users [ bids [ items ] ] ]
		[ bids [ items [ users ] ] ]
		[ users [ bids [ items [ users ] ] ] ]
		QueryPlan [candidates=[ users [ bids ] ] -> [ items ] -> [ users ], indexes=[]]
		QueryPlan [candidates=[ users [ bids [ items [ users ] ] ] ] -> [ items ] -> [ users ], indexes=[]]  ---
		QueryPlan [candidates=[ users ] -> [ bids ] -> [ users [ bids [ items ] ] ] -> [ users ], indexes=[]]
		QueryPlan [candidates=[ users ] -> [ bids ] -> [ users [ bids [ items [ users ] ] ] ], indexes=[]]
		QueryPlan [candidates=[ users ] -> [ bids ] -> [ items ] -> [ users ], indexes=[]]
		QueryPlan [candidates=[ users [ bids [ items ] ] ] -> [ users ], indexes=[]]
		QueryPlan [candidates=[ users ] -> [ bids ] -> [ items [ users ] ], indexes=[]]
		QueryPlan [candidates=[ users ] -> [ bids [ items [ users ] ] ], indexes=[]]
		QueryPlan [candidates=[ users [ bids [ items [ users ] ] ] ], indexes=[]]
		QueryPlan [candidates=[ users ] -> [ bids [ items ] ] -> [ users ], indexes=[]]

	 */
	@Test
	public void testCandidateGenerator() {
		Sequence seq = usersBidsItemsSeller;
		SequenceCandidateGenerator generator = new SequenceCandidateGenerator(seq);
		
		generator.startGeneration();
		
		for(CandidateSequence candidate: generator.getCandidates()) {
			System.out.println(candidate);
		}
		
		for(QueryPlan<CandidateSequence> qp: generator.getExistingQueryPlans()){
			System.out.println(qp);
		}
		
		//TODO validate output
//		assertEquals(12, generator.getExistingQueryPlans().size());
		
	}
}
