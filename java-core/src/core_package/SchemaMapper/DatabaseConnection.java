package core_package.SchemaMapper;

import core_package.Schema.Attribute;
import core_package.Schema.Table;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

public abstract class DatabaseConnection {

    public static String MICROSOFT_SQL_SERVER="MICROSOFT_SQL_SERVER";
    public static String ORACLE="ORACLE";
    public static String MYSQL="MYSQL";

    public static String ATTRIBUTE_TYPE_CATEGORICAL="ATTRIBUTE_TYPE_CATEGORICAL";
    public static String ATTRIBUTE_TYPE_NUMERICAL="ATTRIBUTE_TYPE_NUMERICAL";
    public static String ATTRIBUTE_TYPE_DATE="ATTRIBUTE_TYPE_DATE";

    protected Connection con;

    public static DatabaseConnection getConnectionForDBType(String dbType) { //returns a connection object for the specified dbType string
        if(dbType.equals(MICROSOFT_SQL_SERVER)) {
            return new MSSQLDatabaseConnection();
        }
        else
            return null;
    }

    public abstract ArrayList<Table> getTables() throws Exception;

    public abstract String buildSQLToRetrieveTables();

    public abstract String getPrimaryKeyNameForTable(Table t) throws Exception;

    public abstract String buildSQLToRetrievePrimaryKeyFromTable(String tableName);

    public abstract String getAttributeType(String attributeName, String tableName) throws Exception;

    public abstract String buildSQLToGetAttributeType(String attributeName, String tableName);

    public abstract ArrayList<Attribute> getTableAttributes(String tableName) throws Exception;

    public abstract String buildSQLToGetTableAttributes(String tableName);

    public abstract ResultSet query(String sql) throws Exception;


}
