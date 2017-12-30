package core_package.QueryGeneration;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.jpl7.Term;

import core_package.Schema.Table;

public class QueryBuilder {
	private static Boolean debug = false;
	
	/**
	 * Builds queries from one template file
	 * @param targetTableName the name of the target table, which will be bound to T0
	 * @param templateFile the prolog file relative to the query
	 * @param eng the Prolog engine
	 * @return all queries that can be formed
	 * @throws IOException
	 * @throws MalformedGoalException 
	 * @throws NoSolutionException 
	 * @throws NoMoreSolutionException 
	 */
	public static ArrayList<Query> buildQueries(String targetTableName, String templateFile) throws IOException {
		ArrayList<Query> result = new ArrayList<>();
		BufferedReader br = new BufferedReader(new FileReader(templateFile));
		String code = "T0='"+targetTableName+"',\n";
		String curLine ;
		while ((curLine = br.readLine()) != null) {
				code += curLine + "\n";
		}
		System.out.println("The code is " + (org.jpl7.Query.hasSolution(code) ? "provable" : "not provable"));
		System.out.println("Code: " + code);
		Map<String, Term>[] ss4 = org.jpl7.Query.allSolutions(code);
		
		for (int i=0;i<ss4.length;i++) {
			Map<String,Term> res =  ss4[i];
			String sql = res.get("Query").toString();
			sql = sql.replaceAll("\\n", "\n");
			String description = res.get("Description").toString();
			Double complexity = Double.parseDouble(res.get("Complexity").toString());
			Query q = new Query(sql,complexity,description);
			result.add(q);
		}


		
		
//		while (query.hasMoreChoicePoints()) {
//			System.out.println("Next solution");
//			JIPTerm t=  query.nextSolution();
//
//			if (t == null) break;
//			
//			Hashtable hash = t.getVariablesTable();
//			String sql = hash.get("Query").toString().replaceAll("\\n", "\n");
//			String description = hash.get("Description").toString();
//			Double complexity = Double.parseDouble(hash.get("Complexity").toString());
//			Query q = new Query(sql,complexity,description);
//			result.add(q);
//			if (debug) {
//			
//				Enumeration keys = hash.keys(); 
//				while(keys.hasMoreElements()) {
//					Object k = keys.nextElement();
//					Object v = hash.get(k);
//					System.out.println("k="+k.toString()+" - v="+v.toString());
//				}
//			}
//		}
		return result;
	}
}
