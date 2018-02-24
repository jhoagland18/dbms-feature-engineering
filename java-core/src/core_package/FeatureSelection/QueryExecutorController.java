package core_package.FeatureSelection;

//import sun.plugin.com.ParameterListCorrelator;

import core_package.Environment;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.QueryGeneration.Query;
import core_package.Schema.Attribute;
import core_package.Schema.Table;
import core_package.SchemaBuilder.DatabaseConnection;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class QueryExecutorController {

    private ExecutorService executor;

    private ArrayList<Future> futures = new ArrayList<java.util.concurrent.Future>();
    private ArrayList<QueryExecutor> executors = new ArrayList<QueryExecutor>();

    private ArrayList<DatabaseConnection> connections = new ArrayList<DatabaseConnection>();

    private ArrayList<Query> savedQueries = new ArrayList<>();

    private HashMap<String,Double> target = new HashMap();

    private ArrayList<Query> queries;

    private String targetTableName;
    private String targetTablePK;
    private String targetColName;

    private int numThreads;

    private int progress = 0;
    private int lastUpateValue = 0;

    DatabaseConnection conn;

    HashSet<Integer> ancestors = new HashSet<Integer>();


    public QueryExecutorController(int numThreads,String targetTableName, String targetTablePK, String targetColName, ArrayList<Query> queries) {
        this.targetColName = targetColName;
        this.targetTableName = targetTableName;
        this.targetTablePK = targetTablePK;
        this.numThreads = numThreads;
        this.queries = queries;
    }

    public QueryExecutorController setDatabaseConnection(DatabaseConnection conn) {
        this.conn = conn;
        return this;
    }

    public QueryExecutorController buildTargetHashMap() {
        try {
            ResultSet rs = conn.query("SELECT [" + targetTablePK + "], [" + targetColName +
                    "] FROM " + targetTableName);

            Double value = 0.0;
            while (rs.next()) {
                value = rs.getDouble(2);
                if (!rs.wasNull()) {
                    target.put(rs.getString(1), value);
                    //System.out.println("adding "+value+", "+rs.getString(2));
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }

    public QueryExecutorController runCorrelationAnalysis() {

        executor = Executors.newFixedThreadPool(numThreads);

        //System.out.println("Total number of features to compare: "+queries.size());
        for (int i = 0; i < numThreads; i++) {

            int listStart = i * (queries.size() / numThreads);

            int listEnd = 0;
            if (i == numThreads - 1) {
                listEnd = queries.size();
            } else {
                listEnd = (i + 1) * (queries.size() / numThreads);
            }

            List<Query> partition = queries.subList(listStart, listEnd);
            //System.out.println("Creating new QE: "+listStart+", "+listEnd);
            QueryExecutor qe = null;
            try {
                qe = new QueryExecutor(new ArrayList<Query>(partition), target, this);
            } catch (NoSuchDatabaseTypeException e) {
                e.printStackTrace();
            }

            futures.add(executor.submit(qe));
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < savedQueries.size(); i++) {
            Query q1 = savedQueries.get(i);

            for(int j=i+1; j<savedQueries.size(); j++) {
                Query q2 = savedQueries.get(j);

                double corrToFeature = QueryExecutor.getCorrelationToFeature(q1,q2);
//                System.out.println("Corr to feature is "+corrToFeature);

                if(corrToFeature> Environment.maxFeatureCorrelation) {
                    if(q1.getCorrelationToDependent()>q2.getCorrelationToDependent()) {
                        savedQueries.remove(j);
                        j--;
                    } else {
                        savedQueries.remove(i);
                        i--;
                        break;
                    }
                }
            }

        }

        sortQueriesByCorrelationToDependant(savedQueries);

        System.out.println(savedQueries.size() + " saved queries.");

//        System.out.println("Final Queries: "+savedQueries.size());

//        for(Query q: savedQueries) {
//
//            System.out.println("\nCorr: "+q.getCorrelationToDependent()+"\nSQL:\n"+q.getSQL());
//        }

        writeFeatureValueSheet();
        return this;
    }


    public synchronized void addNewPotentialFeatures(ArrayList<Query> qs) {
        savedQueries.addAll(qs);
    }

    private void writeFeatureValueSheet() {

        ArrayList<HashMap<String,Double[]>> queryRows = new ArrayList<>(savedQueries.size());

        for(Query q: savedQueries) {
            queryRows.add(q.getRows());
        }

        PrintWriter rowWriter = null;
        PrintWriter dictWriter = null;
        
        try {
            rowWriter = new PrintWriter("output/features/Feature Matrix.csv", "UTF-8");
            dictWriter = new PrintWriter("output/features/Feature Dictionary.csv", "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        
        dictWriter.println("Attribute_ID\tAttribute_Descr\tSQL_Query");
        StringBuilder rowLine = new StringBuilder();
        StringBuilder dictLine = new StringBuilder();

        rowLine.append("ID");
        int attNum = 0;

        for(Query q: savedQueries) {
            String attName = "att_"+attNum;
            String desc = q.getDescription();
            desc = desc.replaceAll("\n", " ");
            desc = desc.replaceAll("'", "");
            String sql = q.getSQL();
            sql = sql.replaceAll("\n", " ");
            sql = sql.replaceAll("'", "");
            dictLine.setLength(0);

            rowLine.append(","+attName);

            dictLine.append(attName+"\t");
            dictLine.append(desc+"\t");
            dictLine.append(sql);
            dictWriter.println(dictLine);

            attNum++;
        }

        dictWriter.close();

        rowLine.append(","+targetColName);

        rowWriter.println(rowLine.toString());

        Set<String> ids = target.keySet();

        for(String id: ids) {
            rowLine.setLength(0);

            rowLine.append(id);

            for(HashMap<String, Double[]> row: queryRows) {
                if(row.get(id)[0]==null) {
                    rowLine.append(",");
                } else {
                    rowLine.append("," + row.get(id)[0]);
                }
            }

            rowLine.append(","+target.get(id));

            rowWriter.println(rowLine.toString());

        }
        rowWriter.close();
    }

    private void sortQueriesByCorrelationToDependant(ArrayList<Query> queries) {

        int n = queries.size();

        for (int i=1; i<n; ++i)
        {
            Query key = queries.get(i);
            int j = i-1;

        /* Move elements of arr[0..i-1], that are
           greater than key, to one position ahead
           of their current position */
            while (j>=0 && queries.get(j).getCorrelationToDependent() < key.getCorrelationToDependent())
            {
                queries.set(j+1,queries.get(j));

                j = j-1;
            }
            queries.set(j+1, key);
        }
    }

    public synchronized void submitProgress(int numQueriesComplete) {
        progress+=numQueriesComplete;
        if(((double)progress/(double)queries.size()*100.0)-10>lastUpateValue) {
            lastUpateValue = (int)((double) progress / (double) queries.size() * 100.0);
            System.out.println(lastUpateValue+"%");
            return;
        } else if(((double)progress/(double)queries.size()*100.0)>=100) {
            System.out.println("100%");
        }
    }


}
