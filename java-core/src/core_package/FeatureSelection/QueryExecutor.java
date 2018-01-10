package core_package.FeatureSelection;

import core_package.Environment;
import core_package.QueryGeneration.Query;
import core_package.SchemaBuilder.DatabaseConnection;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class QueryExecutor implements Runnable {

    private final ArrayList<Query> queries;
    private double correlationToDependant;
    private QueryExecutorController queryExecutorController;
    private HashMap<String,Double> target;

    private ArrayList<Query> savedQueries;
    private DatabaseConnection conn;

    public QueryExecutor(ArrayList<Query> queries, HashMap<String, Double> target, QueryExecutorController qec) {
        this.queryExecutorController = qec;
        this.queries = queries;
        conn = queryExecutorController.checkoutConnection();
        this.target = target;
    }

    /**
     * For each Query q in queries that has a minimum correlation with the 
     * dependent variable, find the saved query qs such that: (1) corr(q,qs) > th2, 
     * (2) qs is the most correlated saved query to q. 
     * If q is a better predictor than qs, save q and discard qs.
     * If you can't find any saved query such that corr(q,qs) > th2, then save q.
     */
    @Override
    public void run() {
        savedQueries = new ArrayList<>();
        for (Query q : queries) {
    		// text manipulation
            String sql = q.getSQL().substring(q.getSQL().indexOf("SELECT"));
            sql = sql.substring(0,sql.lastIndexOf('\''));
            
            // run it
            ResultSet rs;

            try {
                 rs = conn.query(sql);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }

            // scan the result and build predictor and dependent
            ArrayList<Double> predictor = new ArrayList<Double>();
            ArrayList<Double> dependent = new ArrayList<Double>();
            try {
                String key = "";
                while(rs.next()) {
                    key = rs.getString(1);
                    if(target.containsKey(key)) {
                        predictor.add(Double.parseDouble(rs.getString(2)));
                        dependent.add(target.get(key));
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            double[] dpredictor = new double[predictor.size()];
            for (int i = dpredictor.length-1; i >= 0; i--) {
                dpredictor[i] = predictor.remove(i);                // java 1.5+ style (outboxing)
            }

            double[] ddependant = new double[dependent.size()];
            for (int i = ddependant.length-1; i >= 0; i--) {
                ddependant[i] = dependent.remove(i);                // java 1.5+ style (outboxing)
            }

            // compute corr(q,target)
            PearsonsCorrelation pc = new PearsonsCorrelation();
            double corr = pc.correlation(dpredictor,ddependant);
            System.out.println("Corr is "+corr);
            if(corr > Environment.highCorrelationWarningThreshold) {
            	System.out.println("This query has a high correlation. I'll dicard it: \n" +sql);
            	continue; // run the next query
            }
            
            if (corr < Environment.minCorrelation)
            	continue;
            
            boolean correlatedAttributeFound = false;
            // compute correlation vs every saved attribute
            for (Query qs : savedQueries) {
//            	//...
//            	 For each saved attribute b:
//            	       if |corr(a,b)| > th2:
//            	            correlatedAttributeFound = true;
//            	            keep only a or b, whichever is more correlated to target
//            	            break
//            	   if !correlatedAttributeFound:
//            	       add a to the set of saved attributes
            }
    	}

    }
    
    /**
     * 
     * @return two arrays (predictor and dependent variable)
     */
    public void runQuery(ArrayList<Double> predictor, ArrayList<Double> dependant) {




        queryExecutorController.returnConnection(conn);
    }

    private double executeQuery() {
        //execute query

        return 0;
    }

    public String getQuery() {
        return query;
    }

    public double getCorrelation() {
        return correlationToDependant;
    }
}
