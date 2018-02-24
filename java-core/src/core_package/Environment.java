package core_package;

public class Environment {
    //Number of cpu cores available.
    public static int MAX_THREADS=1;

    public static String dbName = "cacao";
    public static String targetTableName = "BinarizedRatings";
    public static String targetTablePK = "RatingID";
    public static String targetColName = "NewRating";

    //sql connection url
    public static String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;databasename="+dbName+";integratedSecurity=true";
    
    public static Double highCorrelationWarningThreshold = 0.8;
    public static Double minCorrelation = 0.01;
    public static Double maxFeatureCorrelation = 0.5;

    public static Double maxNullsPercentage = 0.98;

    public static Double maxPercentageForNominal = 0.3;
}
