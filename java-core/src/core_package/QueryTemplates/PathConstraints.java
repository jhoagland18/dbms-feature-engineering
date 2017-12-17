package core_package.QueryTemplates;
import java.util.ArrayList;

import core_package.Pathfinding.Path;
import core_package.Schema.*;

/**
 * This class provides the functionalities needed to check whether a given path matches relationship and table constraints.
 * @author Michele Samorani
 *
 */
public class PathConstraints {
	private ArrayList<RelationshipType> relationships;
	private ArrayList<int []> equalToConstraints;
	private ArrayList<int []> differentFromConstraints;
	
	public PathConstraints() {
		relationships = new ArrayList<>();
		equalToConstraints = new ArrayList<>();
		differentFromConstraints = new ArrayList<>();
	}
	
	/**
	 * Adds a relationship type to the constraint.
	 * 
	 * @param t the relationship type to add
	 */
	public void addRelationship (RelationshipType t) {
		relationships.add(t);
	}
	
	/**
	 * Adds the constraint that two tables along the path be equal to each other.
	 * @param table1Index
	 * @param table2Index
	 */
	public void addEqualToConstraint (int table1Index, int table2Index) {
		equalToConstraints.add(new int [] {table1Index, table2Index});
	}
	
	
	/**
	 * Adds the constraint that two tables along the path be different from each other.
	 * @param table1Index
	 * @param table2Index
	 */
	public void addDifferentFromConstraint (int table1Index, int table2Index) {
		differentFromConstraints.add(new int [] {table1Index, table2Index});
	}
	
	/**
	 * 
	 * @param p the path to check
	 * @return true if p satisfies all table and relationship constraints.
	 */
	public Boolean matches(Path p) {
		// check length
		if (p.getRelationships().size() != relationships.size())
			return false;
		
		// check relationship types
		for (int i =0 ; i< p.getRelationships().size();i++) {
			RelationshipType t = p.getRelationships().get(i).getRelationshipType();
			if (t != relationships.get(i))
				return false;
		}
		
		// check equalToConstraints
		for (int [] constraint : equalToConstraints) {
			int i = constraint[0];
			int j = constraint[1];
			if (p.getTables().get(i) != p.getTables().get(j))
				return false;
		}
		
		// check differentFromConstraints
		for (int [] constraint : differentFromConstraints) {
			int i = constraint[0];
			int j = constraint[1];
			if (p.getTables().get(i) == p.getTables().get(j))
				return false;
		}
		
		return true;
	}
}
