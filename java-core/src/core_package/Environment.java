package core_package;

public class Environment {
    //Number of cpu cores available.
    public static int MAX_THREADS=1;

    //Control how verbose console output is. Primarily for debugging.
    public static int PRINT_MODE_VERBOSE=1;
    public static int PRINT_MODE_NORMAL=0;

    //Pathfinding variables
    public static int MAX_PATH_DEPTH=3;


    public static String dbName = "Dataconda";

    //sql connection url
    public static String dbUserName = "jackson";
    public static String dbPassword = "oF0SDv"; //my unique password for this project
    public static String sqlConnectionUrl = "jdbc:sqlserver://localhost:1433;databasename=Dataconda;integratedSecurity=true"; 
    		
//    		"jdbc:sqlserver://localhost:1433;" +
//            "databaseName=" + dbName + ";user="+dbUserName+";password="+dbPassword;

    public static String targetTableName="Purchases";
    
    public static Double highCorrelationWarningThreshold = 0.8;
    public static Double minCorrelation = 0.05;
    public static Double maxFeatureCorrelation = 0.5;
}
