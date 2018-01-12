package core_package.FeatureSelection;

import core_package.Environment;
import core_package.QueryGeneration.Query;
import core_package.SchemaBuilder.DatabaseConnection;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

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
        savedQueries = new ArrayList<Query>();

        for (int i1 = 0; i1 < queries.size(); i1++) {
            Query q = queries.get(i1);
            // text manipulation
            String sql = q.getSQL().substring(q.getSQL().indexOf("SELECT"));
            sql = sql.substring(0, sql.lastIndexOf('\''));

            // run it
            ResultSet rs;

            try {
                rs = conn.query(sql);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            // compute corr(q,target)
            PearsonsCorrelation pc = new PearsonsCorrelation();

            double corr = getCorrelationToDependant(rs,q);

            System.out.println(q.getSQL());

            System.out.println("Corr is " + corr);

            if (corr > Environment.highCorrelationWarningThreshold) {
                System.out.println("This query has a high correlation. I'll discard it: \n" + sql);
                continue; // run the next query
            }

            if (corr < Environment.minCorrelation)
                continue;

            q.setCorrelationToDependent(corr);

            boolean correlatedAttributeFound = false;

            if(savedQueries.size()==0) {
                savedQueries.add(q);
            }
            // compute correlation vs every saved attribute
            for (int i = 0; i < savedQueries.size(); i++) {
                Query qs = savedQueries.get(i);
                if (q != qs) {
                    if (getCorrelationToFeature(q, qs) > Environment.maxFeatureCorrelation) {
                        if (q.getCorrelationToDependent() > qs.getCorrelationToDependent()) {
                            correlatedAttributeFound = true;
                            savedQueries.set(i, q);
                            break;
                        }
                    }

                }
            }
            if (!correlatedAttributeFound) {
                savedQueries.add(q);
            }

//            	//...
//            	 For each saved attribute b:
//            	       if |corr(a,b)| > th2:
//            	            correlatedAttributeFound = true;
//            	            keep only a or b, whichever is more correlated to target
//            	            break
//            	   if !correlatedAttributeFound:
//            	       add a to the set of saved attributes
        }
        System.out.println("Saved queries: "+savedQueries.size());
        for(Query qs: savedQueries) {
            System.out.println("Corr is: "+qs.getCorrelationToDependent()+" SQL: "+qs.getSQL());
        }

    }

    public double getCorrelationToDependant(ResultSet rs, Query q) {
        ArrayList<Double> predictor = new ArrayList<Double>();
        ArrayList<Double> dependent = new ArrayList<Double>();

        double sx = 0.0;
        double sy = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        double sxy = 0.0;
        int numRowsCompared = 0;

        try {

            HashMap<String,Double> queryRows = new HashMap<String, Double>();

            while(rs.next()) {
                String id = rs.getString(1);
                String value = rs.getString(2);

                if(value!=null) {
                    double valueAsDouble = Double.parseDouble(value);
                    queryRows.put(id, valueAsDouble);
                    if (target.containsKey(id)) {
                        numRowsCompared++;
                        double x = Double.parseDouble(value);
                        double y = target.get(id);

                        sx += x;
                        sy += y;
                        sxx += x * x;
                        syy += y * y;
                        sxy += x * y;
                    }
                }
            }
            q.setRows(queryRows);
        } catch (Exception e) {
            e.printStackTrace();
            return 0.0;
        }

        // covariation
        double cov = sxy / numRowsCompared - sx * sy / numRowsCompared / numRowsCompared;
        // standard error of x
        double sigmax = Math.sqrt(sxx / numRowsCompared -  sx * sx / numRowsCompared / numRowsCompared);
        // standard error of y
        double sigmay = Math.sqrt(syy / numRowsCompared -  sy * sy / numRowsCompared / numRowsCompared);

        // correlation is just a normalized covariation
        return cov / sigmax / sigmay;

    }

    public double getCorrelationToFeature(Query q1, Query q2) {
        HashMap<String,Double> q1Rows = q1.getRows();
        HashMap<String,Double> q2Rows = q2.getRows();

        Set<String> q1keys = q1Rows.keySet();

        double sx = 0.0;
        double sy = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        double sxy = 0.0;
        int numRowsCompared = 0;

        for(String key: q1keys) {
            if(q2Rows.containsKey(key)) {
                numRowsCompared++;
                double x = q1Rows.get(key);
                double y = q2Rows.get(key);

                sx += x;
                sy += y;
                sxx += x * x;
                syy += y * y;
                sxy += x * y;
            }
        }

        // covariation
        double cov = sxy / numRowsCompared - sx * sy / numRowsCompared / numRowsCompared;
        // standard error of x
        double sigmax = Math.sqrt(sxx / numRowsCompared -  sx * sx / numRowsCompared / numRowsCompared);
        // standard error of y
        double sigmay = Math.sqrt(syy / numRowsCompared -  sy * sy / numRowsCompared / numRowsCompared);

        // correlation is just a normalized covariation
        return cov / sigmax / sigmay;
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

    public double getCorrelation() {
        return correlationToDependant;
    }
}
