package core_package.QueryGeneration;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import com.ugos.jiprolog.engine.JIPEngine;
import com.ugos.jiprolog.engine.JIPQuery;
import com.ugos.jiprolog.engine.JIPTerm;
import com.ugos.jiprolog.engine.JIPTermParser;

import core_package.Schema.*;

public class DB2PrologLoader {
	public static JIPEngine LoadDB(String functionsFileName, ArrayList<Table> tables, 
			ArrayList<Relationship> relationships) {
		JIPEngine e = new JIPEngine();
		e.consultFile(functionsFileName);
		// scan DB
		
		JIPTermParser tp = e.getTermParser();
		for (Table t : tables) {
			// PK
			String pks = makePrologList(t.getPrimaryKey(), true);
			e.asserta(tp.parseTerm("pk('"+t.getName()+"',"+pks+")."));
			
			// scan all non-ID attributes of table t
			for (Attribute a : t.getAttributes()) {
				if (a.getClass() != IDAttribute.class)
					e.asserta(tp.parseTerm("attribute('"+t.getName()+"', '"+
					a.getAttributeName() +"', '" + typeOfAtt(a) + "', '" + a.getDimension() + "')."));
				// declare bins for numeric attributes
				if (a.getClass() == NumericAttribute.class) {
					NumericAttribute na = (NumericAttribute)a;
					String bins = makePrologList(na.getBinThresholds(), false);
					e.asserta(tp.parseTerm("bin_thresholds('"+t.getName()+
							"','"+na.getAttributeName()+"',"+bins+")."));
				}
				// declare important values for nominal attributes
				if (a.getClass() == NominalAttribute.class) {
					NominalAttribute na = (NominalAttribute)a;
					String impvals = makePrologList(na.getImportantValues(),true);
					e.asserta(tp.parseTerm("important_values('"+t.getName()+
							"','"+na.getAttributeName()+"',"+impvals+")."));
				}

			}
		}
		
		// relationships
		for (Relationship r : relationships) {
			String fk1 = makePrologList(r.getAttributes1(), true);
			String fk2 = makePrologList(r.getAttributes2(), true);
			e.asserta(tp.parseTerm("relationship('"+r.getTable1().getName()+"','"
					+ r.getTable2().getName() +"',"+fk1+","+fk2+","+r.toString()+")."));
		}
		
		return e;
	}

	private static String makePrologList(ArrayList<?> values, boolean quotes) {
		String q = quotes? "'" : "";
		String ret = "[";
		for (int i=0;i<values.size()-1;i++)
			ret= ret+ q + values.get(i) +q + ",";
		ret =ret+ q+values.get(values.size()-1) +q+"]";
		return ret;
	}

	private static String typeOfAtt(Attribute a) {
		if (a.getClass() == NominalAttribute.class)
			return "nominal";
		if (a.getClass() == NumericAttribute.class)
			return "numeric";
		if (a.getClass() == DateAttribute.class)
			return "date";
		if (a.getClass() == TimeStampAttribute.class)
			return "timestamp";
		if (a.getClass() == ZeroOneAttribute.class)
			return "zero_one";
		if (a.getClass() == IDAttribute.class)
			return "id";
		else return "notype";
	}
}
