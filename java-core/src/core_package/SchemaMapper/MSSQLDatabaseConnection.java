package core_package.SchemaMapper;


import core_package.Environment;
import core_package.Main;
import core_package.Schema.Attribute.Attribute;
import core_package.Schema.Attribute.CategoricalAttribute;
import core_package.Schema.Attribute.DateAttribute;
import core_package.Schema.Attribute.NumericalAttribute;
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

    private static String sqlGetTables = "SELECT TABLE_NAME FROM FEResearch.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'";
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

    public ArrayList<Table> getTables() throws SQLException { //Returns a list of tables that is added to DBSchema in SchemaBuilder
        ArrayList<Table> detectedTables = new ArrayList<Table>();
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sqlGetTables);

        while (rs.next()) {
            detectedTables.add(new Table(rs.getString(1)));
        }

        return detectedTables;
    }

    @Override
    public String getPrimaryKeyNameForTable(Table t)  {
        String attName="";
        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sqlGetPrimaryKeyForTable1 + t.getTableName() + sqlGetPrimaryKeyForTable2);
            if(rs.next()) {
                attName = rs.getString(1);
            }
        } catch(Exception e) {
            System.out.println("sql: "+sqlGetPrimaryKeyForTable1 + t.getTableName() + sqlGetPrimaryKeyForTable2);
            e.printStackTrace();
        }

        return attName;
    }

    @Override
    public String getAttributeType(String attributeName, String tableName) throws Exception {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery(sqlGetAttributeDataType1 + tableName + sqlGetAttributeDataType2 + attributeName + sqlGetAttributeDataType3);

        String attType = "";
        if(rs.next()) {
           attType = rs.getString(1);

        } else {
            throw new Exception("No datatype returned");
        }
        if(attType.equals(DATA_TYPE_INT) || attType.equals(DATA_TYPE_DOUBLE))
        {
            return DatabaseConnection.ATTRIBUTE_TYPE_NUMERICAL;
        } else if(attType.equals(DATA_TYPE_VARCHAR)) {
            return DatabaseConnection.ATTRIBUTE_TYPE_CATEGORICAL;
        } else if(attType.equals(DATA_TYPE_DATETIME)) {
            return DatabaseConnection.ATTRIBUTE_TYPE_DATE;
        } else {
            throw new Exception("No such datatype");
        }
    }

    @Override
    public ArrayList<Attribute> getTableAttributes(String tableName) throws Exception {
        Statement stmt = con.createStatement();
        ResultSet rs=null;
        rs = stmt.executeQuery(sqlGetTableColumns1+tableName+sqlGetTableColumns2);

        String attName="";
        String attType="";
        ArrayList<Attribute> newAttributes = new ArrayList<Attribute>();

        Main.printVerbose(sqlGetTableColumns1+tableName+sqlGetTableColumns2);

        int i=0;

        while(rs.next()) {
            attName = rs.getString(1);
            attType = getAttributeType(attName, tableName);

            if(attType == DatabaseConnection.ATTRIBUTE_TYPE_NUMERICAL)
            {
                newAttributes.add(new NumericalAttribute(attName));
            } else if(attType == DatabaseConnection.ATTRIBUTE_TYPE_CATEGORICAL) {
                newAttributes.add(new CategoricalAttribute(attName));
            } else if(attType == DatabaseConnection.ATTRIBUTE_TYPE_DATE) {
                newAttributes.add(new DateAttribute(attName));
            } else {
                throw new Exception("No such datatype: "+attType);
            }
        }

        return newAttributes;
    }
}
