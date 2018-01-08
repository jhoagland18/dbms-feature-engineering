package core_package.SchemaBuilder.Connection;


import core_package.Environment;
import core_package.Schema.Table;
import core_package.SchemaBuilder.DatabaseConnection;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class MSSQLDatabaseConnection extends DatabaseConnection {

    private static String[] sqlGetTables = new String[] {"SELECT TABLE_NAME FROM "+Environment.dbName+".INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"};

    private static String[] sqlGetPrimaryKeyForTable = new String[] {"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu ON tc.CONSTRAINT_NAME = ccu.Constraint_name WHERE tc.CONSTRAINT_TYPE = 'Primary Key' and tc.TABLE_NAME = '","'"};

    private static String[] sqlGetAttributeDataType = new String[] {"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '","' AND COLUMN_NAME = '", "'"};

    private static String[] sqlGetTableColumns = new String[] {"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'", "'"};

    private static String[] sqlGetTableAttributesNameAndDataType = new String[] {"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'", "'"};

    private static String[] sqlGetDifferenceBetweenUniqueAndTotalOccurances = new String[] {"Select count(distinct [", "])-count(", ") from "};

    private static String[] sqlGetNumUniqueRows = new String[] {"Select count(distinct [", "]) from "};

    private static String[] sqlGetUniqueRows= new String[] {"Select distinct [","] from "};



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
        return sqlGetTables[0];
    }

    @Override
    public String buildSQLToRetrievePrimaryKeyFromTable(String tableName) {
        return sqlGetPrimaryKeyForTable[0] + tableName + sqlGetPrimaryKeyForTable[1];
    }


    @Override
    public String buildSQLToGetAttributeDataType(String attributeName, String tableName) {
        return sqlGetAttributeDataType[0] + tableName + sqlGetAttributeDataType[1] + attributeName + sqlGetAttributeDataType[2];
    }

    @Override
    public String buildSQLToGetTableAttributes(String tableName) {
        return sqlGetTableColumns[0]+tableName+sqlGetTableColumns[1];
    }

    @Override
    public String buildSQLToGetTableAttributeNameAndDatatype(String tableName) {
        return sqlGetTableAttributesNameAndDataType[0]+tableName+sqlGetTableAttributesNameAndDataType[1];
    }

    @Override
    public String buildSQLToGetDifferenceBetweenTotalAndNumUnique(String attName, Table t) {
        return sqlGetDifferenceBetweenUniqueAndTotalOccurances[0] + attName + sqlGetDifferenceBetweenUniqueAndTotalOccurances[1] + attName + sqlGetDifferenceBetweenUniqueAndTotalOccurances[2] + t.getName();
    }

    @Override
    public String buildSQLToGetNumUniqueRows(String attName, Table t) {
        return sqlGetNumUniqueRows[0] + attName + sqlGetNumUniqueRows[1] + t.getName();
    }

    @Override
    public String buildSQLToGetUniqueRows(String attName, Table t) {
        return sqlGetUniqueRows[0] + attName + sqlGetUniqueRows[1] + t.getName();
    }

    @Override
    public ResultSet query(String sql) {
        ResultSet rs = null;
        try {
            Statement stmt = con.createStatement();
            rs = stmt.executeQuery(sql);
        } catch(Exception e) {
            System.out.println("Error when executing query. SQL: "+sql);
            e.printStackTrace();
        }
        return rs;
    }

}
