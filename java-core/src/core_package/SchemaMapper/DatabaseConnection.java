package core_package.SchemaMapper;

import core_package.Exception.*;
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

    //database data types
    protected static String DATA_TYPE_INT = "int";
    protected static String DATA_TYPE_DOUBLE = "double";
    protected static String DATA_TYPE_VARCHAR = "varchar";
    protected static String DATA_TYPE_DATETIME = "datetime";

    //attribute type
    public static String ATTRIBUTE_TYPE_CATEGORICAL="ATTRIBUTE_TYPE_CATEGORICAL";
    public static String ATTRIBUTE_TYPE_NUMERICAL="ATTRIBUTE_TYPE_NUMERICAL";
    public static String ATTRIBUTE_TYPE_DATE="ATTRIBUTE_TYPE_DATE";



    protected Connection con;

    /**
     * returns a connection object for the specified dbType string
     * @param dbType
     * @return
     */
    public static DatabaseConnection getConnectionForDBType(String dbType) throws NoSuchDatabaseTypeException {
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
     * executes given SQL string parameter and returns a ResultSet containing the repsonse's data
     * @param sql
     * @return
     * @throws SQLException
     */
    public abstract ResultSet query(String sql) throws SQLException;


}
