package core_package.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;


public class NumericAttribute extends Attribute {
	
	ArrayList<Double> binThresholds;
	
	/**
	 * 
	 * @return the bin thresholds used in where conditions
	 */
	public ArrayList<Double> getBinThresholds() {
		return binThresholds;
	}
	
	/**
	 * 
	 * @param attributeName
	 * @param binThresholds the bin thresholds used in where conditions. They must be sorted in ascending order. 
	 * If for instance, binThreholds is composed of v1,v2,v3,v4, the where conditions generated will be 
	 * x &lt v1, v1 &lt= x &lt v2, v2 &lt= x &lt v3, v3 &lt= x &lt v4, x &gt v4
	 */
	public NumericAttribute(String attributeName, String dimension, ArrayList<Double> binThresholds)  {
		super(attributeName,dimension);
		ArrayList<Double> copyArr = new ArrayList<>(binThresholds);
		Collections.sort(copyArr);
		this.binThresholds = copyArr;
	}

}
