package core_package.SchemaBuilder;

import core_package.Exception.*;
import core_package.Schema.Table;
import core_package.SchemaBuilder.Connection.MSSQLDatabaseConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public abstract class DatabaseConnection {

    //Database name
    public static String MICROSOFT_SQL_SERVER="MICROSOFT_SQL_SERVER";
    public static String ORACLE="ORACLE";
    public static String MYSQL="MYSQL";



    //attribute type
    public static String ATTRIBUTE_TYPE_CATEGORICAL="ATTRIBUTE_TYPE_CATEGORICAL";
    public static String ATTRIBUTE_TYPE_NUMERICAL="ATTRIBUTE_TYPE_NUMERICAL";
    public static String ATTRIBUTE_TYPE_DATE="ATTRIBUTE_TYPE_DATE";



    protected Connection con;
    private String connType;

    /**
     * returns a connection object for the specified dbType string
     * @param dbType
     * @return
     */
    public static DatabaseConnection getConnectionForDBType(String dbType) throws NoSuchDatabaseTypeException {
        //System.out.println("Getting new database connection for "+dbType);
        if(dbType.equals(MICROSOFT_SQL_SERVER)) {
            return new MSSQLDatabaseConnection();
        }
        else
            throw new NoSuchDatabaseTypeException("Database type " + dbType + " does not exist");
    }

    /**
     * returns SQL to retrieve one column containing the connected database's tables
     * @return
     */
    public abstract String buildSQLToRetrieveTables();

    /**
     * returns SQL to retrieve primary key from designated table
     * @param tableName
     * @return
     */
    public abstract String buildSQLToRetrievePrimaryKeyFromTable(String tableName);

    /**
     * returns SQL to retrieve attribute datatype from designated attribute
     * @param attributeName
     * @param tableName
     * @return
     */
    public abstract String buildSQLToGetAttributeDataType(String attributeName, String tableName);

    /**
     * returns SQL to retrieve a single column of table attributes
     * @param tableName
     * @return
     */
    public abstract String buildSQLToGetTableAttributes(String tableName);

    /**
     * returns two column resultset containing table attribtue names, and corresponding table attributes
     * @param tableName
     * @return
     */
    public abstract String buildSQLToGetTableAttributeNameAndDatatype(String tableName);


    public abstract String buildSQLToGetDifferenceBetweenTotalAndNumUnique(String attName, Table t);

    public abstract String buildSQLToGetNumUniqueRows(String attName, Table t);

    public abstract String buildSQLToGetUniqueRows(String attName, Table t);

    public abstract String buildSQLToGetTopXRowsByNewID(String attName, int numRows, Table t);

    public abstract String buildSQLToGetTopXRowsOfTableByNewID(ArrayList<String> attnames, int numRows, Table t);

    /**
     * executes given SQL string parameter and returns a ResultSet containing the repsonse's data
     * @param sql
     * @return
     * @throws SQLException
     */
    public abstract ResultSet query(String sql) throws SQLException, Exception;

    public abstract ResultSet query(String sql, Statement stmt) throws SQLException;

    public abstract Connection getConnection();


}
