package core_package.SchemaBuilder;
import core_package.Environment;
import core_package.Exception.NoPrimaryKeyIdentifiedException;
import core_package.Exception.NoSuchCardinalityException;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.Exception.NoSuchTableException;
import core_package.Schema.*;
import org.w3c.dom.Attr;

import javax.naming.directory.NoSuchAttributeException;
import java.io.*;
import java.lang.reflect.Constructor;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Period;
import java.util.*;

public class SchemaBuilder {

    private ArrayList<Double> presetBins = new ArrayList<Double>();
    private ArrayList<String> presetImportantValues = new ArrayList<>();
    private ArrayList<Period> presetPeriods = new ArrayList<>();

    private static String ATTRIBUTE_TYPE_ID = "id";
    private static String ATTRIBUTE_TYPE_NOMINAL = "nominal";
    private static String ATTRIBUTE_TYPE_NUMERIC = "numeric";
    private static String ATTRIBUTE_TYPE_ZEROONE = "zeroone";
    private static String ATTRIBUTE_TYPE_TIMESTAMP = "timestamp";

    //database data types

    //numeric data types
    protected static String DATA_TYPE_INT = "int";
    protected static String DATA_TYPE_BIG_INT = "bigint";
    protected static String DATA_TYPE_BIT_INT = "bit";
    protected static String DATA_TYPE_SMALL_INT = "smallint";
    protected static String DATA_TYPE_TINY_INT = "tinyint";
    protected static String DATA_TYPE_DECIMAL = "decimal";
    protected static String DATA_TYPE_MONEY = "money";
    protected static String DATA_TYPE_NUMERIC = "numeric";
    protected static String DATA_TYPE_FLOAT = "float";
    protected static String DATA_TYPE_REAL = "real";
    protected static final Set<String> DATA_TYPES_NUMERIC = new HashSet<String>(Arrays.asList(
            new String[] {DATA_TYPE_INT, DATA_TYPE_BIG_INT, DATA_TYPE_SMALL_INT, DATA_TYPE_TINY_INT,
            DATA_TYPE_BIT_INT, DATA_TYPE_DECIMAL, DATA_TYPE_MONEY, DATA_TYPE_NUMERIC, DATA_TYPE_FLOAT, DATA_TYPE_REAL}));

    //string data types
    protected static String DATA_TYPE_CHAR = "char";
    protected static String DATA_TYPE_VARCHAR = "varchar";
    protected static String DATA_TYPE_TEXT = "text";
    protected static String DATA_TYPE_NTEXT = "ntext";
    protected static final Set<String> DATA_TYPES_TEXT = new HashSet<String>(Arrays.asList(
            new String[] {DATA_TYPE_CHAR, DATA_TYPE_VARCHAR, DATA_TYPE_TEXT, DATA_TYPE_NTEXT}));

    //time data types
    protected static String DATA_TYPE_TIME = "time";
    protected static String DATA_TYPE_DATETIME = "datetime";
    protected static String DATA_TYPE_DATE = "date";
    protected static final Set<String> DATA_TYPES_TIME = new HashSet<String>(Arrays.asList(
            new String[]{DATA_TYPE_TIME, DATA_TYPE_DATETIME, DATA_TYPE_DATE}));

    private static String delimiter = "@@@";

    private Schema schema;
    private DatabaseConnection conn;

    public SchemaBuilder() {
        schema = new Schema();
    }

    public SchemaBuilder(String connectionType) throws NoSuchDatabaseTypeException {
        this();
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public void setConnectionType(String connectionType) throws NoSuchDatabaseTypeException {
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    public SchemaBuilder(DatabaseConnection conn) {
        this();
        this.conn = conn;
    }

    public Schema getSchema() {
        return schema;
    }

    public SchemaBuilder buildSchema() throws Exception {

        loadRelationships();

        for(Table t: schema.getTables()) {
            sampleRowsForTable(t,5000);
        }

        for(Table t: schema.getTables()) { //iterates through tables and adds attributes and primary keys
            addAttributesToTable(t);
        }

        for(Table t: schema.getTables()) {
            writeTableAttributes(t);
        }

        return this;
    }

    public SchemaBuilder loadSchema() throws Exception {

        loadRelationships();

        loadTables();

        return this;
    }

    private void loadTables() throws Exception {

        ArrayList<String> tableNames = new ArrayList<>();
        for(Table t:this.getSchema().getTables()) {
            tableNames.add(t.getName());
        }

        File tablesDir = new File("schema\\tables");
        File[] tableFiles = tablesDir.listFiles();

        BufferedReader br;
        String line = "";
        //String csvDelimiter = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

        if (tableFiles != null) {

            for (File tableFile : tableFiles) {
                br = new BufferedReader(new FileReader(tableFile.getAbsolutePath()));

                String nameWithExt = tableFile.getName();
                String nameWithoutExt = nameWithExt.substring(0,nameWithExt.lastIndexOf("."));
                Table t = this.getSchema().getTableByName(nameWithoutExt);

                while ((line = br.readLine()) != null) {
                    String[] data = line.split(delimiter,-1);
                    System.out.println("line " +Arrays.toString(data));
                    System.out.println(data[0]);
                    String attName = data[0];
                    String attType = data[1];

                    if(attName.startsWith("\"") && attName.endsWith("\"")) {
                        attName.substring(1);
                        attName.substring(0,attName.length());
                    }
                    ArrayList<String> attBins = new ArrayList<>();

                    for (int i = 2; i < data.length; i++) {
                        attBins.add(data[i]);
                    }

                    Attribute newAtt = null;

                    if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_ID)) {
                        continue;
//                        newAtt = new IDAttribute(attName);
                    } else if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NOMINAL)) {
                        newAtt = new NominalAttribute(attName,null,attBins);
                    } else if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NUMERIC)) {
                        ArrayList<Double> doubleBins = new ArrayList<>();
                        for(String s:attBins) {
                            doubleBins.add(Double.parseDouble(s));
                        }
                        newAtt = new NumericAttribute(attName,null,doubleBins);
                    } else if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_TIMESTAMP)) {
                        ArrayList<Period> periodBins = new ArrayList<>();
                        for(String s:attBins) {
                            periodBins.add(Period.parse(s));
                        }
                        newAtt = new TimeStampAttribute(attName,periodBins);
                    } else if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_ZEROONE)) {
                        newAtt = new ZeroOneAttribute(attName);
                    }

                    if(newAtt==null) {
                        System.out.println("newatt is "+newAtt+" "+attName+ " type: "+attType);
                    }

                    t.addAttribute(newAtt);
                }


            }
        } else {
            // Handle the case where dir is not really a directory.
            // Checking dir.isDirectory() above would not be sufficient
            // to avoid race conditions with another process that deletes
            // directories.
        }
    }



    private void loadRelationships() throws Exception {
        String filePath = "schema\\relationships.csv";

        BufferedReader br = null;
        String line = "";
        String csvDelimiter = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

        try {
            br = new BufferedReader(new FileReader(filePath));

            while ((line = br.readLine()) != null) {
                String[] data = line.split(csvDelimiter,-1);
                String t1Name = data[0];
                String t2Name = data[1];
                boolean t1keyIsPk = data[2].startsWith("#") && data[2].endsWith("#");
                boolean t2keyIsPk = data[3].startsWith("#") && data[3].endsWith("#");
                if(t1keyIsPk) {
                    data[2] = data[2].substring(1);
                    data[2] = data[2].substring(0,data[2].length()-1);
                }
                if(t2keyIsPk) {
                    data[3] = data[3].substring(1);
                    data[3] = data[3].substring(0,data[3].length()-1);
                }
                String t1key = "["+data[2]+"]";
                String t2key = "["+data[3]+"]";
                String cardinality = data[4];

                try {
                    Table t1 = schema.getTableByName(t1Name);

                    if(t1==null) {
                        t1 = new Table(t1Name);
                        schema.addTable(t1);
                    }

                    Table t2 = schema.getTableByName(t2Name);

                    if(t2==null) {
                        t2 = new Table(t2Name);
                        schema.addTable(t2);
                    }

                    if(t1.getPrimaryKey().size()==0 && t1keyIsPk) {
                        t1.setPrimaryKey(new IDAttribute(t1key));
                    } else if(t1keyIsPk && !tableContainsPrimaryKey(t1,t1key)){
                        if(tableContainsPrimaryKey(t1,t1key)) {
                            ArrayList<Attribute> pks = t1.getPrimaryKey();
                            pks.add(new IDAttribute(t1key));
                            t1.setPrimaryKey(pks);
                        }
                    }

                    if(t2.getPrimaryKey().size()==0 && t2keyIsPk) {
                        t2.setPrimaryKey(new IDAttribute(t2key));
                    } else if(t2keyIsPk && !tableContainsPrimaryKey(t2,t2key)){
                        if(tableContainsPrimaryKey(t2,t2key)) {
                            ArrayList<Attribute> pks = t2.getPrimaryKey();
                            pks.add(new IDAttribute(t2key));
                            t2.setPrimaryKey(pks);
                        }
                    }



                    IDAttribute att1=null;
                    IDAttribute att2=null;

                    if(!tableContainsAttribute(t1,t1key)) { //add fk to t2 if not already in
                        t1.addAttribute(new IDAttribute(t1key));
                    }

                    if(!tableContainsAttribute(t2,t2key)) { //add fk to t2 if not already in
                        t2.addAttribute(new IDAttribute(t2key));
                    }

                    att1 = (IDAttribute) t1.getAttributeByName(t1key);

                    att2 = (IDAttribute) t2.getAttributeByName(t2key);

                    RelationshipType type = null;

                    if (cardinality.equalsIgnoreCase("ToN")) {
                        type = RelationshipType.ToN;
                    } else if (cardinality.equalsIgnoreCase("To1")) {
                        type = RelationshipType.To1;
                    } else {
                        throw new NoSuchCardinalityException(cardinality + " is not a valid cardinality. Check the cardinality csv. Valid options are \"To1\" and \"ToN\".");
                    }

                    Relationship r = new Relationship(t1, t2, att1 , att2, type);

                    t1.addRelationship(r);


                } catch (NoSuchTableException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        for(Table t:this.getSchema().getTables()) {
            if(t.getName().equalsIgnoreCase(Environment.targetTableName)) {
                if(!tableContainsPrimaryKey(t,"["+Environment.targetTablePK+"]")) {
                    ArrayList<Attribute> pks = t.getPrimaryKey();
                    pks.add(new IDAttribute("[" + Environment.targetTablePK+"]"));
                    t.setPrimaryKey(pks);
                }
                break;
            }
        }
    }

    private void writeTableAttributes(Table t) throws NoSuchAttributeException, IOException {
        ArrayList<String> pknames = new ArrayList<>();
        String out = "";
        if(t.getPrimaryKey().isEmpty()) {
            System.out.println("PK for "+t.getName()+" is EMPTY!!!");
        }
        for(Attribute att: t.getPrimaryKey()) {
            out+=att.getAttributeName()+delimiter+"id\n";
            pknames.add(att.getAttributeName());
        }

        for(Attribute att: t.getAttributes()) {

            boolean attInTable = false;

            for(String attName: t.getRowSample().keySet()) {
                attName = "["+attName+"]";
                if(attName.equalsIgnoreCase(att.getAttributeName())) {
                    attInTable = true;
                    break;
                }

            }

            if(!attInTable) {
                throw new NoSuchAttributeException("Attribute " + att.getAttributeName() + " could not be found in table " + t.getName() + ". Check the spelling in the relationships csv and try again");
            }

            boolean ispk = false;
            for(String pkname:pknames) {
                if(pkname.equalsIgnoreCase(att.getAttributeName())) {
                    ispk = true;
                    break;
                }
            }

            if(ispk) {
                continue;
            }

            out+=att.getAttributeName()+delimiter;

            if(att instanceof NominalAttribute) {
                out+=ATTRIBUTE_TYPE_NOMINAL;
                //bins
                for(String s: ((NominalAttribute) att).getImportantValues()) {
                	s = s.replace("'", "\\'");
                	if(!s.isEmpty())
                        out+=delimiter+s;
                }
            } else if(att instanceof NumericAttribute) {
                out+=ATTRIBUTE_TYPE_NUMERIC;
                //bins
                for(Double d: ((NumericAttribute) att).getBinThresholds()) {
                    out+=delimiter+d;
                }
            } else if(att instanceof TimeStampAttribute) {
                out+=ATTRIBUTE_TYPE_TIMESTAMP;
                //bins
                for(Period p:((TimeStampAttribute) att).getBinThresholds()) {
                    out+=delimiter+p.toString();
                }
            } else if(att instanceof ZeroOneAttribute) {
                out+=ATTRIBUTE_TYPE_ZEROONE;
            } else if(att instanceof IDAttribute) {
                out+=ATTRIBUTE_TYPE_ID;
            }
            out+="\n";
        }

        File tableFile = new File("schema\\tables\\"+t.getName()+".csv\\");
        tableFile.getParentFile().mkdirs();
        tableFile.createNewFile();

        FileWriter writer = new FileWriter(tableFile);
        writer.write(out);
        writer.close();

    }

    private void sampleRowsForTable(Table t, int numToSample) throws Exception {
        HashMap<String, HashMap<String,Integer>> sampleRows = new HashMap<>();

        ResultSet rs = conn.query(conn.buildSQLToGetTableAttributeNameAndDatatype(t.getName())); //col 1 is name, col 2 is data type

        while(rs.next()) {
            sampleRows.put(rs.getString(1),new HashMap<String,Integer>());
        }

        ArrayList<String> attNames = new ArrayList<>();
        for(String s: sampleRows.keySet()) {
            System.out.print(" "+s);
            attNames.add(s);
        }
        System.out.println();

        Statement stmt = conn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet tableSample = conn.query(conn.buildSQLToGetTopXRowsOfTableByNewID(attNames,numToSample,t),stmt);

        int col = 2; //1 is newID
        for(int i=0; i<attNames.size();i++) { //iterate through att names
            HashMap<String,Integer> values = new HashMap<>();
            tableSample.first();
            while(tableSample.next()) {
                String key = tableSample.getString(col);
                if(values.containsKey(key)) {
                    values.put(key,values.get(key)+1);
                } else {
                    values.put(key,1);
                }
            }
            sampleRows.put(attNames.get(i),values);
            col++;
        }

        t.setRowSample(sampleRows);
    }

    private void addAttributesToTable(Table t) throws Exception {
        Set<String> attributeNames = t.getRowSample().keySet();

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        ArrayList<Attribute> tablePks = t.getPrimaryKey();

        ArrayList<String> tablePkNames = new ArrayList<>();

        for(Attribute a:tablePks) {
            tablePkNames.add(a.getAttributeName());
        }

        System.out.println("PKs for "+t.getName()+" are "+tablePkNames.toString());

        for(String attributeName: attributeNames) {
            if(tableContainsAttribute(t,"["+attributeName+"]")) {
                continue;
            }



            boolean isNumeric = true;
            boolean isBinary = true;
            boolean isTimeStamp = true;

            HashMap<String,Integer> values = t.getRowSample().get(attributeName);

            int numRows = 0;

            for(String value: values.keySet()) {

                numRows+=values.get(value);

                if(isNumeric) {
                	try {
                		double v = Double.parseDouble(value);

                        if(isBinary) {
                            if(v!=0 && v!=1) { //if not zero and not one
                                isBinary = false;
                            }
                        }

                	}catch(Exception e) {
                	    isNumeric = false;
                	    isBinary = false;
                	}
                }

                if(isTimeStamp) {
                    try {
                        format.parse(value);
                    } catch (ParseException e) {
                        isTimeStamp = false;
                    }
                }
            }

            attributeName = "["+attributeName+"]";

            int numUniqueValues = values.keySet().size();

            //System.out.println("for "+attributeName+" there are "+numRows + " rows "+ " and "+numUniqueValues + " unique values.");

            if(isBinary) {
                t.addAttribute(new ZeroOneAttribute(attributeName));
            } else if(isTimeStamp) {
                //TODO calculate bins
                t.addAttribute(new TimeStampAttribute(attributeName,presetPeriods));
            } else if(isNumeric) {
                //TODO calculate bins
                ArrayList<Double> numericBins = new ArrayList<>();

                //simple method to create 10 bins, each 1/10th of the max - min value
                double max = Double.MIN_VALUE;
                double min = Double.MAX_VALUE;

                double valueD;
                for(String value: values.keySet()) {
                    valueD = Double.parseDouble(value);
                    if(valueD>max) {
                        max = valueD;
                    }

                    if(valueD<min) {
                        min = valueD;
                    }
                }

                double increment = Math.abs((max-min))/10.0d;
                for(int i=0;i<10;i++) {
                    numericBins.add(min + (increment*i));
                }

                t.addAttribute(new NumericAttribute(attributeName,null,numericBins));
            } else {
                //TODO what to do if nothing is common enough to be an important value?
                ArrayList<String> nominalBins = new ArrayList<>();
                for(String value: values.keySet()) {
                    if(values.get(value)>(double)numRows*0.00d) {
                        nominalBins.add(value);
                    }
                }
                t.addAttribute(new NominalAttribute(attributeName,null,nominalBins));
            }
        }
    }

    public boolean isUnique(String attName, Table t) throws Exception {
        ResultSet rs = conn.query(conn.buildSQLToGetDifferenceBetweenTotalAndNumUnique(attName, t));
        int diff = Integer.parseInt(rs.getString(1));
        return diff==0 ? true : false;
    }

    public boolean tableContainsAttribute(Table t, String attName) {
        for(Attribute att: t.getAttributes()) {
            if(att.getAttributeName().equalsIgnoreCase(attName)) {
                return true;
            }
        }
        return false;
    }

    public boolean tableContainsPrimaryKey(Table t, String attName) {
        for(Attribute att: t.getPrimaryKey()) {
            if(att.getAttributeName().equalsIgnoreCase(attName)) {
                return true;
            }
        }
        return false;
    }

}
