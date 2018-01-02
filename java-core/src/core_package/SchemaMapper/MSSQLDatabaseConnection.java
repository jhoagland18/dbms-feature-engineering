package core_package.SchemaMapper;


import core_package.Environment;
import core_package.Main;
import core_package.Schema.Attribute;
import core_package.Schema.Table;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class MSSQLDatabaseConnection extends DatabaseConnection {

    private static String DATA_TYPE_INT = "int";
    private static String DATA_TYPE_DOUBLE = "double";
    private static String DATA_TYPE_VARCHAR = "varchar";
    private static String DATA_TYPE_DATETIME = "datetime";

    private static String sqlGetTables = "SELECT TABLE_NAME FROM "+Environment.dbName+".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
    private static String sqlGetPrimaryKeyForTable1 = "SELECT COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc " +
            "JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu " +
            "ON tc.CONSTRAINT_NAME = ccu.Constraint_name " +
            "WHERE tc.CONSTRAINT_TYPE = 'Primary Key' and tc.TABLE_NAME = '";
    private static String sqlGetPrimaryKeyForTable2 = "'";

    private static String sqlGetAttributeDataType1 = "SELECT DATA_TYPE " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_NAME = '";
    private static String sqlGetAttributeDataType2 = "' AND COLUMN_NAME = '";
    private static String sqlGetAttributeDataType3 = "'";

    private static String sqlGetTableColumns1 = "SELECT COLUMN_NAME " +
            "FROM INFORMATION_SCHEMA.COLUMNS " +
            "WHERE TABLE_NAME = N'";
    private static String sqlGetTableColumns2 = "'";


    public MSSQLDatabaseConnection() {
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); //connect to driver
            con = DriverManager.getConnection(Environment.sqlConnectionUrl);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @return
     */
    @Override
    public String buildSQLToRetrieveTables() {
        return sqlGetTables;
    }

    @Override
    public String buildSQLToRetrievePrimaryKeyFromTable(String tableName) {
        return sqlGetPrimaryKeyForTable1 + tableName + sqlGetPrimaryKeyForTable2;
    }


    @Override
    public String buildSQLToGetAttributeDataType(String attributeName, String tableName) {
        return sqlGetAttributeDataType1 + tableName + sqlGetAttributeDataType2 + attributeName + sqlGetAttributeDataType3;
    }

    @Override
    public String buildSQLToGetTableAttributes(String tableName) {
        return sqlGetTableColumns1+tableName+sqlGetTableColumns2;
    }

    @Override
    public ResultSet query(String sql) throws Exception{
        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

}
