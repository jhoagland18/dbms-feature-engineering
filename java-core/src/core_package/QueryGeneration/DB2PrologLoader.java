package core_package.QueryGeneration;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Period;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

import core_package.Schema.*;

public class DB2PrologLoader {
	public static void LoadDB(String functionsFileName, ArrayList<Table> tables, 
			ArrayList<Relationship> relationships) throws IOException {
		
		// copy functions into theory.pl
		Files.copy(new File(functionsFileName).toPath(), new File("prolog/theory.pl").toPath(),
				StandardCopyOption.REPLACE_EXISTING);
		
		// scan DB and append the theory on prolog\theory.pl
		FileWriter fw = new FileWriter("prolog/theory.pl", true);
	    BufferedWriter bw = new BufferedWriter(fw);
	    PrintWriter writer = new PrintWriter(bw);
	
	    boolean declaredTimestampPeriods = false;
		for (Table t : tables) {
			// PK
			String pks = makePrologList(t.getPrimaryKey(), true);
			String clause = "pk('"+t.getName()+"',"+pks+").";
			writer.println(clause);
			
			// add PKs as nominal
			for (Attribute a : t.getPrimaryKey()) {
				clause = "attribute(\'"+t.getName()+"\', \'"+a.getAttributeName()+"\', \'nominal\', \'null\').";
				writer.println(clause);
			}
			
			// scan all non-ID attributes of table t
			for (Attribute a : t.getAttributes()) {
				if (a.getClass() != IDAttribute.class) {
					clause = "attribute('"+t.getName()+"', '"+
							a.getAttributeName() +"', '" + typeOfAtt(a) + "', '" + a.getDimension() + "').";
					writer.println(clause);
				}
				// declare bins for numeric attributes
				if (a.getClass() == NumericAttribute.class) {
					NumericAttribute na = (NumericAttribute)a;
					String bins = makePrologList(na.getBinThresholds(), false);
					clause = "bin_thresholds('"+t.getName()+
							"','"+na.getAttributeName()+"',"+bins+").";
					writer.println(clause);
				}
				// declare important values for nominal attributes
				if (a.getClass() == NominalAttribute.class) {
					NominalAttribute na = (NominalAttribute)a;
					String impvals = makePrologList(na.getImportantValues(),true);
					clause = "important_values('"+t.getName()+
							"','"+na.getAttributeName()+"',"+impvals+").";
					writer.println(clause);
				}

				// declare important values for timestamp attributes
//				if (a.getClass() == TimeStampAttribute.class && !declaredTimestampPeriods) {
//					TimeStampAttribute na = (TimeStampAttribute)a;
//					String impvals = makePrologListOfPeriods(na.getBinThresholds());
//					clause = "timestamp_periods("+impvals+").";
//					writer.println(clause);
//					declaredTimestampPeriods = true;
//				}
			}
		}
		
		// relationships
		for (Relationship r : relationships) {
			String fk1 = makePrologList(r.getAttributes1(), true);
			String fk2 = makePrologList(r.getAttributes2(), true);
			String clause = "relationship('"+r.getTable1().getName()+"','"
					+ r.getTable2().getName() +"',"+fk1+","+fk2+","+r.toString()+").";
			writer.println(clause);		
		}
		writer.close();
		bw.close();
		fw.close();
		
		String t1 = "consult('prolog/theory.pl')";
		System.out.println(t1 + " " + (org.jpl7.Query.hasSolution(t1) ? "succeeded" : "failed"));

	}

	/**
	 * Makes a prolog list of periods. It returns also the period length in SQL format: e.g. [1,2,3],'mm'.
	 * 
	 * @param values
	 * @return
	 */
	private static String makePrologListOfPeriods(ArrayList<Duration> values) {
		// find the smallest period
		Duration [] toTestAgainst = new Duration[] {Duration.ofMillis(1), 
				Duration.ofSeconds(1), 
				Duration.ofMinutes(1), 
				Duration.ofHours(1), 
				Duration.ofDays(1), 
				Duration.of(1,ChronoUnit.WEEKS), 
				Duration.of(1,ChronoUnit.MONTHS), 
				Duration.of(1,ChronoUnit.YEARS)};

		// find the largest unit of time that is smaller than all of the interesting durations
		Duration chosen = Duration.ofNanos(1);
		boolean mustExit = false;
		for (Duration p : toTestAgainst) {
			for (Duration v : values)
				if (mustExit)
					break;
				else if (p.compareTo(v) > 0) {
					mustExit = true;
				}
			chosen = p;
		}
		
		// TODO finish
		return "";
	}

	/**
	 * Makes a prolog list [a,b,c,...].
	 * @param values
	 * @param quotes If true, elements will be printed within single quotes
	 * @return
	 */
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
		if (a.getClass() == TimeStampAttribute.class)
			return "timestamp";
		if (a.getClass() == ZeroOneAttribute.class)
			return "zero_one";
		if (a.getClass() == IDAttribute.class)
			return "id";
		else return "notype";
	}
}
