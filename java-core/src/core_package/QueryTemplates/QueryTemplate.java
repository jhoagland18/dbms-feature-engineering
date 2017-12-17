package core_package.QueryTemplates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import core_package.Pathfinding.Path;
import core_package.Schema.*;

public class QueryTemplate {
	PathConstraints pathConstaints;
	
	// T[0] is purchases, T[1] is clients, etc
	ArrayList<Table> tableNameBindings;
	
	// A is (Price, Return), etc
	HashMap<String, ArrayList<Attribute>> attributeBindings;
	
	// AGG is (MIN,MAX,SUM,AVG)
	HashMap<String, ArrayList<String>> userDefinedBindings;
	
	String queryTemplateString;
	
	public QueryTemplate(PathConstraints pathConstaints, String queryTemplateString) {
		tableNameBindings = new ArrayList<>();
		attributeBindings = new HashMap<>();
		userDefinedBindings = new HashMap<>();
		pathConstaints = this.pathConstaints;
		this.queryTemplateString = queryTemplateString;
	}
	
	public ArrayList<Query> getQueries(Path p) {
		// if the path doesn't match, return an empty set of queries
		if (!pathConstaints.matches(p))
			return new ArrayList<>();
		
		String q2 = PopulateTableNameBindings(p);
		
		String q3 = ResolveTableFunctions(q2,p);
		return null;
	}

	/**
	 * modifies the input query so that the table-related functions are populated
	 * @param q
	 * @param p
	 * @return
	 */
	private String ResolveTableFunctions(String q, Path p) {
		return null;
	}

	/**
	 * Change Ti with the name of the ith table in the path
	 * @param p
	 */
	private String PopulateTableNameBindings(Path p) {
		String res = queryTemplateString;
		
		for (int i=0;i<p.getLength();i++)
			res.replaceAll("\\bT"+i+"\\b", p.getTables().get(i).getName());
		return res;
	}
}
