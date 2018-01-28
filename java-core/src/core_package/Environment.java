package core_package;

public class Environment {
    //Number of cpu cores available.
    public static int MAX_THREADS=1;

    public static String dbName = "cacao";

    //sql connection url
    public static String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;databasename="+dbName+";integratedSecurity=true";
    
    public static Double highCorrelationWarningThreshold = 0.8;
    public static Double minCorrelation = 0.05;
    public static Double maxFeatureCorrelation = 0.5;
}
