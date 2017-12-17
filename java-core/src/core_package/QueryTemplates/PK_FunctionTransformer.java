package core_package.QueryTemplates;

import java.util.HashMap;

import core_package.Pathfinding.Path;
import core_package.Schema.Attribute;
import core_package.Schema.Table;

public class PK_FunctionTransformer extends AbstractFunctionTransformer {

	@Override
	protected String Transform(String[] arguments, HashMap<String,Table> tableDict, 
			HashMap<String,Attribute> attributeDict, Path p) {
		Table t = tableDict.get(arguments[0]);
		String symbolicName = arguments[1];
		String res = symbolicName + "." + t.getPrimaryKey().get(0).getAttributeName();
		for (int i=1;i<t.getPrimaryKey().size();i++) 
			res += "," + symbolicName + "." + t.getPrimaryKey().get(i).getAttributeName();
		return res;
	}

	@Override
	protected String GetFunctionName() {
		return "PK";
	}

}
