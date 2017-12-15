package core_package.Schema;

import java.util.ArrayList;
import java.time.Period;

/**
 * An attribute that represents the moment when a row was available. There can only be at most one such attribute per table.
 *
 */
public class TimeStampAttribute extends Attribute {
	
	ArrayList<Period> binThresholds;
	
	/**
	 * 
	 * @return the bin thresholds used in where conditions
	 */
	public ArrayList<Period> getBinThresholds() {
		return binThresholds;
	}

	/**
	 * 
	 * @param attributeName
	 * @param binThresholds the bin thresholds used in where conditions involving two timestamps t1 and t2 of the same dimension. They must be sorted in ascending order. 
	 * If for instance, binThreholds is composed of v1,v2,v3,v4, the where conditions generated will be 
	 * t1-t2 &lt v1, v1 &lt= t1-t2 &lt v2, v2 &lt= t1-t2 &lt v3, v3 &lt= t1-t2 &lt v4, t1-t2 &gt v4
	 */
	public TimeStampAttribute(String attributeName, ArrayList<Period> binThresholds)  {
		super(attributeName,"timestamp");
		this.binThresholds = binThresholds;
	}

}
