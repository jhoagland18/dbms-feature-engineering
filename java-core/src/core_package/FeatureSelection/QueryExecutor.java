package core_package.FeatureSelection;

import core_package.Environment;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.QueryGeneration.Query;
import core_package.SchemaBuilder.DatabaseConnection;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class QueryExecutor implements Runnable {

    private final ArrayList<Query> queries;
    private double correlationToDependant;
    private QueryExecutorController queryExecutorController;
    private HashMap<String,Double> target;

    private int numQueriesComplete = 0;
    private int numQueriesNotified = 0;

    private ArrayList<Query> savedQueries;
    private DatabaseConnection conn;

    private HashMap<String, Double[]> predictorValues = new HashMap<String, Double[]>(); //[0] = actual, including nulls: [1] = all absent rows averaged

    long sqltime = 0L;

    long dependenttime = 0L;

    long featurecomparisontime = 0L;

    public QueryExecutor(ArrayList<Query> queries, HashMap<String, Double> target, QueryExecutorController qec) throws NoSuchDatabaseTypeException {
        this.queryExecutorController = qec;
        this.queries = queries;
        conn = DatabaseConnection.getConnectionForDBType(DatabaseConnection.MICROSOFT_SQL_SERVER);
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

        for(Query q: queries) {
            numQueriesComplete++;

            if((numQueriesComplete%(queries.size()/8))==0 || numQueriesComplete==queries.size()) {
                int numCompletedSinceLast = numQueriesComplete-numQueriesNotified;
                queryExecutorController.submitProgress(numCompletedSinceLast);
                numQueriesNotified = numQueriesComplete;
            }
            // text manipulation
            String sql = q.getSQL().substring(q.getSQL().indexOf("SELECT"));
            sql = sql.substring(0, sql.lastIndexOf('\''));

            // run it
            ResultSet rs;

            long startsqltime = System.nanoTime();

            try {
                rs = conn.query(sql);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }

            sqltime += System.nanoTime() - startsqltime;

            long startcorrtodept = System.nanoTime();
            double corr = getCorrelationToDependant(rs,q);

            Set<String> keys = predictorValues.keySet();
            ArrayList<String> keys2 = new ArrayList<>(keys);

            double absCorr = Math.abs(corr);
            dependenttime += System.nanoTime() - startcorrtodept;

//            System.out.println("Correlation is " + corr);

            if (absCorr > Environment.highCorrelationWarningThreshold) {
                System.out.println("This query has a high correlation. I'll discard it: \n" + sql);
                continue; // run the next query
            }

            if (Double.isNaN(absCorr) ||  absCorr < Environment.minCorrelation)
                continue;

            q.setCorrelationToDependent(corr);
            q.setRows(predictorValues);

            boolean correlatedAttributeFound = false;

            if(savedQueries.size()==0) {
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
            // compute correlation vs every saved attribute

            long featurecomparestart = System.nanoTime();

            int highestCorrIndex = 0;
            double highestCorr = 0.0;
            for (int i = 0; i < savedQueries.size(); i++) {
                Query qs = savedQueries.get(i);
                double corrToFeature = Math.abs(getCorrelationToFeature(q,qs));
                if (corrToFeature > Environment.maxFeatureCorrelation) {
                    if (q.getCorrelationToDependent() >= qs.getCorrelationToDependent()) {
                        if(corrToFeature>highestCorr) {
                            highestCorr = corrToFeature;
                            highestCorrIndex=i;
                            correlatedAttributeFound=true;
                        }
                    }
                }

            }

            featurecomparisontime += System.nanoTime() - featurecomparestart;

            if(correlatedAttributeFound) {
                savedQueries.set(highestCorrIndex,q);
            } else {
                savedQueries.add(q);
            }


        }

//        System.out.println("\nTotal work time for all features was "+(featurecomparisontime+sqltime+dependenttime)/1000000000.0+".\n"+
//        "Calculating correlation to dependent took: "+dependenttime/1000000000.0+
//                "\nCalculating correlation between selected features took: "+featurecomparisontime/1000000000.0
//                +"\nSQL queries took: "+ sqltime/1000000000.0);

        queryExecutorController.addNewPotentialFeatures(savedQueries);

    }

    public double getCorrelationToDependant(ResultSet rs, Query q) {

        predictorValues.clear();

        double sx = 0.0;
        double sy = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        double sxy = 0.0;
        int numRowsCompared = 0;

        double correlation = 0.0;

        try {
            int numRows = 0;
            double sumRows = 0;
            Double avgRow = 0.0;

            while(rs.next()) {
                double value = rs.getDouble(2);
                if(!rs.wasNull()) {
                    numRows++;
                    sumRows += value;
                    Double[] values = {value,avgRow};
                    predictorValues.put(rs.getString(1), values);
                }
            }

            avgRow = sumRows/numRows;

            Set<String> keys = target.keySet();

            int numNulls = 0;
            int numNotNull = 0;



            for(String key: keys) {

                Double[] values = {null,avgRow};
                if(!predictorValues.containsKey(key)) {
                    predictorValues.put(key,values);
                    numNulls++;
                } else {
                    numNotNull++;
                }

                numRowsCompared++;

                double x = predictorValues.get(key)[1];
                double y = target.get(key);

                sx += x;
                sy += y;
                sxx += x * x;
                syy += y * y;
                sxy += x * y;

                // covariation
                double cov = sxy / numRowsCompared - sx * sy / numRowsCompared / numRowsCompared;
                // standard error of x
                double sigmax = Math.sqrt(sxx / numRowsCompared -  sx * sx / numRowsCompared / numRowsCompared);
                // standard error of y
                double sigmay = Math.sqrt(syy / numRowsCompared -  sy * sy / numRowsCompared / numRowsCompared);

                // correlation is just a normalized covariation
                correlation =  cov / sigmax / sigmay;
            }

            if(((double)numNulls/(double)numRowsCompared)>Environment.maxNullsPercentage) {
                return 0.0;
            }

        } catch (Exception e) {
            //e.printStackTrace();
            return 0.0;
        }



        return correlation;

    }

    public static double getCorrelationToFeature(Query q1, Query q2) {
        HashMap<String,Double[]> q1Rows = q1.getRows();
        HashMap<String,Double[]> q2Rows = q2.getRows();

        Set<String> q1keys = q1Rows.keySet();

        double sx = 0.0;
        double sy = 0.0;
        double sxx = 0.0;
        double syy = 0.0;
        double sxy = 0.0;
        int numRowsCompared = 0;

        double correlation = 0.0;

        for(String key: q1keys) {
            if(q2Rows.containsKey(key)) {
                numRowsCompared++;

//                if(numRowsCompared>200 && correlation>Environment.maxFeatureCorrelation) {
//                    return correlation;
//                }

                double x = q1Rows.get(key)[1];
                double y = q2Rows.get(key)[1];

                sx += x;
                sy += y;
                sxx += x * x;
                syy += y * y;
                sxy += x * y;

                // covariation
                double cov = sxy / numRowsCompared - sx * sy / numRowsCompared / numRowsCompared;
                // standard error of x
                double sigmax = Math.sqrt(sxx / numRowsCompared -  sx * sx / numRowsCompared / numRowsCompared);
                // standard error of y
                double sigmay = Math.sqrt(syy / numRowsCompared -  sy * sy / numRowsCompared / numRowsCompared);

                // correlation is just a normalized covariation
                correlation = cov / sigmax / sigmay;
            }
        }

        return correlation;
    }

    private double executeQuery() {
        //execute query

        return 0;
    }

    public double getCorrelation() {
        return correlationToDependant;
    }
}
