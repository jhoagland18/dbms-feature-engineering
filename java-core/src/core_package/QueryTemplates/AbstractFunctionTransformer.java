package core_package.QueryTemplates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import core_package.Schema.*;
import core_package.Pathfinding.*;

public abstract class AbstractFunctionTransformer {
	protected HashMap<String,Table> tableDict;
	protected HashMap<String,Table> attributeDict;
	protected Path p;
		
	public String Transform (String query, HashMap<String,Table> tableDict, HashMap<String,Attribute> attributeDict, Path p) {
		// TODO: it should not return a string. It should return a list of substitution actions:
		// a list of index->list of possible substitutions. Subs = range,new string
		// look for F(...)
		Pattern patt = Pattern.compile(GetFunctionName() + " *(.*)");
		Matcher m = patt.matcher(query);
		ArrayList<int[]> beginEnds = new ArrayList<>();
		while (m.find()){
		    int begin = m.start();
		    int end = m.end();
		    beginEnds.add(new int[] {begin,end});
		}
		String nextStr = query;
		for (int i=beginEnds.size()-1;i>=0;i--) {
		    int begin = beginEnds.get(i)[0];
		    int end = beginEnds.get(i)[1];
		    String s2 = query.substring(begin, end);
		    // replace occurrence with what I need. But first tokenize the arguments
			// find arguments
		    s2 = s2.replace(" ","");
		    String [] args = s2.split("\\(");
		    args= args[1].split("\\)");
		    args = args[0].split(",");
		    nextStr = nextStr.substring(0,begin) + Transform(args, tableDict, attributeDict, p) + nextStr.substring(end,nextStr.length());
		}
		return nextStr;
	}
	
	protected abstract String Transform (String [] arguments, HashMap<String,Table> tableDict, HashMap<String,Attribute> attributeDict, Path p);
	protected abstract String GetFunctionName();
}
