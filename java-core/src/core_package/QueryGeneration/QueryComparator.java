package core_package.QueryGeneration;

import java.util.Comparator;

public class QueryComparator implements Comparator<Query> {

	@Override
	public int compare(Query arg0, Query arg1) {
		return arg0.getComplexity().compareTo(arg1.getComplexity());
	}

}
