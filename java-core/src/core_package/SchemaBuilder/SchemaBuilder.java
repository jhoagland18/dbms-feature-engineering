package core_package.SchemaBuilder;
import core_package.Environment;
import core_package.Exception.NoPrimaryKeyException;
import core_package.Exception.NoSuchCardinalityException;
import core_package.Exception.NoSuchDatabaseTypeException;
import core_package.Exception.NoSuchTableException;
import core_package.Schema.*;

import javax.naming.directory.NoSuchAttributeException;
import java.io.*;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Period;
import java.util.*;

public class SchemaBuilder {

    //strings to match in the relationships and table csv files
    private static String ATTRIBUTE_TYPE_ID = "id";
    private static String ATTRIBUTE_TYPE_NOMINAL = "nominal";
    private static String ATTRIBUTE_TYPE_NUMERIC = "numeric";
    private static String ATTRIBUTE_TYPE_ZEROONE = "zeroone";
    private static String ATTRIBUTE_TYPE_TIMESTAMP = "timestamp";

    //set a custom delimiter here
    private static String delimiter = "@@@";

    private Schema schema;

    private DatabaseConnection conn;

    public SchemaBuilder() {
        schema = new Schema();
    }

    /**
     * @param connectionType should be a static string from the DatabaseConnection class of the desired database type
     * @throws NoSuchDatabaseTypeException
     */
    public SchemaBuilder(String connectionType) throws NoSuchDatabaseTypeException {
        this();
        conn = DatabaseConnection.getConnectionForDBType(connectionType);
    }

    /**
     * @param connectionType
     * @throws NoSuchDatabaseTypeException
     */
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

    /**
     * @return returns this for chaining
     * @throws Exception
     */
    public SchemaBuilder buildSchema() throws Exception {

        //reads relationships csv, creates tables if they don't already exist, and adds key attributes to db
        loadRelationships();

        for(Table t: schema.getTables()) {
            //sample X random rows for attribute type identification
            sampleRowsForTable(t,5000);
        }

        for(Table t: schema.getTables()) {
            //Determines attribute type from the sampled rows, generates bins, and adds attribute to table
            addAttributesToTable(t);
        }

        //write tables to csv files
        for(Table t: schema.getTables()) {
            writeTableAttributes(t);
        }

        return this;
    }

    public SchemaBuilder loadSchema() throws Exception {

        //reads relationships csv, creates tables if they don't already exist, and adds key attributes to db
        loadRelationships();

        //reads each table csv in the tables/ directory, adds each table to schema, and adds each attribute in the csv to the table.
        loadTables();

        return this;
    }

    private void loadTables() throws Exception {
        //dir of table files
        File tablesDir = new File("schema\\tables");

        //list of filenames
        File[] tableFiles = tablesDir.listFiles();

        BufferedReader br;
        String line = "";

        if (tableFiles != null) {

            //load attributes from table csv into table
            for (File tableFile : tableFiles) {
                br = new BufferedReader(new FileReader(tableFile.getAbsolutePath()));

                String nameWithExt = tableFile.getName();
                String nameWithoutExt = nameWithExt.substring(0,nameWithExt.lastIndexOf(".")); //truncate name at last '.' to remove file extension
                Table t = this.getSchema().getTableByName(nameWithoutExt);

                //read each line in table csv
                while ((line = br.readLine()) != null) {
                    String[] data = line.split(delimiter, -1);
                    String attName = data[0];
                    String attType = data[1];

                    //unescape if attName is escaped
                    if (attName.startsWith("\"") && attName.endsWith("\"")) {
                        attName.substring(1);
                        attName.substring(0, attName.length());
                    }

                    ArrayList<String> attBins = new ArrayList<>();

                    //add bins to bin arraylist
                    for (int i = 2; i < data.length; i++) {
                        attBins.add(data[i]);
                    }

                    Attribute newAtt = null;

                    //determine type of attribute and create instance to add to table later
                    if (attType.equalsIgnoreCase(ATTRIBUTE_TYPE_ID)) {
                        continue;
                    } else if (attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NOMINAL)) {
                        newAtt = new NominalAttribute(attName, null, attBins);
                    } else if (attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NUMERIC)) {
                        ArrayList<Double> doubleBins = new ArrayList<>();
                        for (String s : attBins) {
                            doubleBins.add(Double.parseDouble(s));
                        }
                        newAtt = new NumericAttribute(attName, null, doubleBins);
                    } else if (attType.equalsIgnoreCase(ATTRIBUTE_TYPE_TIMESTAMP)) {
                        ArrayList<Period> periodBins = new ArrayList<>();
                        for (String s : attBins) {
                            periodBins.add(Period.parse(s));
                        }
                        newAtt = new TimeStampAttribute(attName, periodBins);
                    } else if (attType.equalsIgnoreCase(ATTRIBUTE_TYPE_ZEROONE)) {
                        newAtt = new ZeroOneAttribute(attName);
                    }

                    Attribute existingAtt = t.getAttrbuteByNameIgnoreCase(attName);

                    if (existingAtt == null) { //if attribute is not already in table, add attribute
                        t.addAttribute(newAtt);
                    } else { //if attribute is already in table (was added when loading relationships), copy the new bins into the attribute
                        if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NOMINAL)) {
                            ((NominalAttribute)existingAtt).setImportantValues(((NominalAttribute)newAtt).getImportantValues());
                        } else if(attType.equalsIgnoreCase(ATTRIBUTE_TYPE_NUMERIC)) {
                            ((NumericAttribute)existingAtt).setBinThresholds(((NumericAttribute)newAtt).getBinThresholds());
                        }
                    }
                }
            }

        } else {
            // Handle the case where dir is not really a directory.
            //TODO
        }
    }

    private void loadRelationships() throws Exception {
        //relatonships csv filepath
        String filePath = "schema\\relationships.csv";

        BufferedReader br = null;
        String line = "";
        String csvDelimiter = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; //regex to check for unescaped commas (matches an even number of following double quotes)

        try {
            br = new BufferedReader(new FileReader(filePath));

            //read each line in relationships.csv
            while ((line = br.readLine()) != null) {
                String[] data = line.split(csvDelimiter,-1);
                String t1Name = data[0];
                String t2Name = data[1];
                boolean t1keyIsPk = data[2].startsWith("#") && data[2].endsWith("#"); //flag indicating table 1 key is a primary key
                boolean t2keyIsPk = data[3].startsWith("#") && data[3].endsWith("#"); //flag indicating table 2 key is a primary key

                if(t1keyIsPk) { //remove flag characters from table 1 key
                    data[2] = data[2].substring(1);
                    data[2] = data[2].substring(0,data[2].length()-1);
                }

                if(t2keyIsPk) { //remove flag characters from table 2 key
                    data[3] = data[3].substring(1);
                    data[3] = data[3].substring(0,data[3].length()-1);
                }

                String t1key = encloseInBrackets(data[2]);
                String t2key = encloseInBrackets(data[3]);

                //cardinality flag
                String cardinality = data[4];

                try {
                    Table t1 = schema.getTableByName(t1Name);

                    //if table 1 not in schema, create and add table to schema
                    if(t1==null) {
                        t1 = new Table(t1Name);
                        schema.addTable(t1);
                    }

                    Table t2 = schema.getTableByName(t2Name);

                    //if table 2 not in schema, create and add table to schema
                    if(t2==null) {
                        t2 = new Table(t2Name);
                        schema.addTable(t2);
                    }

                    Attribute t1Att, t2Att;

                    //determine Table 1 key attribute type and create instance to add to table later
                    if(t1key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL))) {
                        t1key = t1key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL),"");
                        t1Att = new NominalAttribute(t1key,null,null);
                    } else if(t1key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC))) {
                        t1key = t1key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC),"");
                        t1Att = new NumericAttribute(t1key,null,null);
                    } else if(t1key.contains(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP))) {
                        t1key = t1key.replace(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP),"");
                        t1Att = new TimeStampAttribute(t1key,new ArrayList<Period>());
                    }  else if(t1key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE))) {
                        t1key = t1key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE),"");
                        t1Att = new ZeroOneAttribute(t1key);
                    } else if(t1key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ID))) {
                        t1key = t1key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ID),"");
                        t1Att = new IDAttribute(t1key);
                    } else {
                        t1Att = new IDAttribute(t1key);
                    }

                    //determine Table 2 key attribute type and create instance to add to table later
                    if(t2key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL))) {
                        t2key = t2key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL),"");
                        t2Att = new NominalAttribute(t1key,null,null);
                    } else if(t2key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC))) {
                        t2key = t2key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC),"");
                        t2Att = new NumericAttribute(t1key,null,null);
                    } else if(t2key.contains(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP))) {
                        t2key = t2key.replace(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP),"");
                        t2Att = new TimeStampAttribute(t1key,new ArrayList<Period>());
                    }  else if(t2key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE))) {
                        t2key = t2key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE),"");
                        t2Att = new ZeroOneAttribute(t1key);
                    } else if(t2key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ID))) {
                        t2key = t2key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ID),"");
                        t2Att = new IDAttribute(t1key);
                    } else {
                        t2Att = new IDAttribute(t2key);
                    }

                    //if Table 1 has no primary key and table 1 key is flagged as primary, set table 1 key as primary key. Else if table 1 key is flagged as pk and table doesn't contain this key already, add as pk to table
                    if(t1.getPrimaryKey().size()==0 && t1keyIsPk) {
                        t1.setPrimaryKey(t1Att);
                    } else if(t1keyIsPk && !tableContainsPrimaryKey(t1,t1key)){
                        if(tableContainsPrimaryKey(t1,t1key)) {
                            ArrayList<Attribute> pks = t1.getPrimaryKey();
                            pks.add(t1Att);
                            t1.setPrimaryKey(pks);
                        }
                    }

                    //if Table 2 has no primary key and table 2 key is flagged as primary, set table 2 key as primary key. Else if table 2 key is flagged as pk and table doesn't contain this key already, add as pk to table
                    if(t2.getPrimaryKey().size()==0 && t2keyIsPk) {
                        t2.setPrimaryKey(t2Att);
                    } else if(t2keyIsPk && !tableContainsPrimaryKey(t2,t2key)){
                        if(tableContainsPrimaryKey(t2,t2key)) {
                            ArrayList<Attribute> pks = t2.getPrimaryKey();
                            pks.add(t2Att);
                            t2.setPrimaryKey(pks);
                        }
                    }

                    if(!t1keyIsPk && !tableContainsAttribute(t1,t1key)) { //Add table 1 key to table 1 if not a primary key and not already in
                        t1.addAttribute(t1Att);
                    }

                    if(!t2keyIsPk && !tableContainsAttribute(t2,t2key)) { //Add table 2 key to table 2 if not a primary key and not already in
                        t2.addAttribute(t2Att);
                    }

                    RelationshipType type = null;

                    //determine cardinality
                    if (cardinality.equalsIgnoreCase("ToN")) {
                        type = RelationshipType.ToN;
                    } else if (cardinality.equalsIgnoreCase("To1")) {
                        type = RelationshipType.To1;
                    } else {
                        throw new NoSuchCardinalityException(cardinality + " is not a valid cardinality. Check the cardinality csv. Valid options are \"To1\" and \"ToN\".");
                    }

                    Relationship r = new Relationship(t1, t2, t1Att , t2Att, type);

                    //add relationship to table 1
                    t1.addRelationship(r);
                } catch (NoSuchTableException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        //get the target table, and then add the target table's primary key to target table if not already in
        for(Table t:this.getSchema().getTables()) {
            if(t.getName().equalsIgnoreCase(Environment.targetTableName)) {
                if(!tableContainsPrimaryKey(t,encloseInBrackets(Environment.targetTablePK))) {
                    String key = encloseInBrackets(Environment.targetTablePK);
                    Attribute pkatt = null;
                    if(key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL))) {
                        key = key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NOMINAL),"");
                        pkatt = new NominalAttribute(key,null,null);
                    } else if(key.contains(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC))) {
                        key = key.replace(encloseInBrackets(ATTRIBUTE_TYPE_NUMERIC),"");
                        pkatt = new NumericAttribute(key,null,null);
                    } else if(key.contains(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP))) {
                        key = key.replace(encloseInBrackets(ATTRIBUTE_TYPE_TIMESTAMP),"");
                        pkatt = new TimeStampAttribute(key,new ArrayList<Period>());
                    }  else if(key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE))) {
                        key = key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ZEROONE),"");
                        pkatt = new ZeroOneAttribute(key);
                    } else if(key.contains(encloseInBrackets(ATTRIBUTE_TYPE_ID))) {
                        key = key.replace(encloseInBrackets(ATTRIBUTE_TYPE_ID),"");
                        pkatt = new IDAttribute(key);
                    } else {
                        pkatt = new IDAttribute(key);
                    }

                    ArrayList<Attribute> pks = t.getPrimaryKey();
                    pks.add(pkatt);
                    t.setPrimaryKey(pks);
                }
                break;
            }
        }
    }

    /**
     * Creates a file for the table specified. Writes the information of each attribute, one per line, in the csv format attname,atttype,bin 1,bin 2,bin n-1,bin n
     * @param t
     * @throws NoSuchAttributeException
     * @throws IOException
     * @throws NoPrimaryKeyException
     */
    private void writeTableAttributes(Table t) throws NoSuchAttributeException, IOException, NoPrimaryKeyException {
        String out = "";
        if(t.getPrimaryKey().isEmpty()) {
            throw new NoPrimaryKeyException("No primary key is set for table "+t.getName()+". Make sure to indicate primary key in relationships.csv by both prepending and appending \'#\' to the attribute name." +
                    "\nIf the table missing a primary key is the target table, make sure the target table primary key is listed.");
        }

        //load all attributes from table
        ArrayList<Attribute> allAtts = new ArrayList<>();
        allAtts.addAll(t.getPrimaryKey());
        allAtts.addAll(t.getAttributes());

        //write attribute to table csv
        for(Attribute att: allAtts) {

            //flag indicating if attribute is already in table
            boolean attInTable = false;

            //determine if user defined attribute name matches an attribute from the table in the database
            for(String attName: t.getRowSample().keySet()) {
                attName = "["+attName+"]";
                if(attName.equalsIgnoreCase(att.getAttributeName())) {
                    attInTable = true;
                    break;
                }

            }

            //throw exception if attribute is not in database table. User probably spelled something wrong or made some other mistake.
            if(!attInTable) {
                throw new NoSuchAttributeException("Attribute " + att.getAttributeName() + " could not be found in table " + t.getName() + ". Check the spelling in the relationships csv and try again");
            }

            //start building line to print
            out+=att.getAttributeName()+delimiter;

            //append attribute type as string and bins (if applicable) to line
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

        //create file
        File tableFile = new File("schema\\tables\\"+t.getName()+".csv\\");
        tableFile.getParentFile().mkdirs();
        tableFile.createNewFile();

        //write line
        FileWriter writer = new FileWriter(tableFile);
        writer.write(out);
        writer.close();

    }

    private void sampleRowsForTable(Table t, int numToSample) throws Exception {
        HashMap<String, HashMap<String,Integer>> sampleRows = new HashMap<>();

        //executes premade query to select attribute names and datatype for table
        ResultSet rs = conn.query(conn.buildSQLToGetTableAttributeNameAndDatatype(t.getName())); //col 1 is name, col 2 is data type

        //insert attribute names into hashmap
        while(rs.next()) {
            sampleRows.put(rs.getString(1),new HashMap<String,Integer>());
        }

        ArrayList<String> attNames = new ArrayList<>();
        for(String s: sampleRows.keySet()) {
            attNames.add(s);
        }

        //create statement and execute query to select random X rows of table attributes
        Statement stmt = conn.getConnection().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        ResultSet tableSample = conn.query(conn.buildSQLToGetTopXRowsOfTableByNewID(attNames,numToSample,t),stmt);

        int col = 2; //index 1 is newID

        //iterate over attribute names, adding a hashmap of values and occurances of the value to the table's attribute hashmap
        for(int i=0; i<attNames.size();i++) {
            HashMap<String,Integer> values = new HashMap<>();
            tableSample.first();

            //iterate over rows in resultset
            while(tableSample.next()) {
                String key = tableSample.getString(col);
                if(values.containsKey(key)) {
                    values.put(key,values.get(key)+1); //increment occurances of value
                } else {
                    values.put(key,1);
                }
            }
            sampleRows.put(attNames.get(i),values);
            col++;
        }

        //store sampled data
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


        for(String attributeName: attributeNames) {

            HashMap<String,Integer> values = t.getRowSample().get(attributeName);

            Attribute att = t.getAttrbuteByNameIgnoreCase("["+attributeName+"]");

            if(att!=null) {
                if(att instanceof NominalAttribute) {
                    if(((NominalAttribute) att).getImportantValues()==null) {

                        int count = 0;

                        for(String key:values.keySet()) {
                            count+=values.get(key);
                        }

                        ((NominalAttribute) att).setImportantValues(getNominalBins(values,count));
                    }
                } else if(att instanceof NumericAttribute) {
                    if(((NumericAttribute) att).getBinThresholds()==null) {
                        int count = 0;

                        for(String key:values.keySet()) {
                            count+=values.get(key);
                        }
                        ((NumericAttribute) att).setBinThresholds(getNumericBins(values));
                    }
                }
                continue;
            }

            //being testing for attribute type
            boolean isNumeric = true;
            boolean isBinary = true;
            boolean isTimeStamp = true;

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

            if(isBinary) { //add binary attribute to table
                t.addAttribute(new ZeroOneAttribute(attributeName));
            } else if(isTimeStamp) { //add timestamp attribute to table
                //TODO calculate bins
                t.addAttribute(new TimeStampAttribute(attributeName,new ArrayList<>()));
            } else if(isNumeric) { //add numeric attribute to table
                //TODO calculate bins
                t.addAttribute(new NumericAttribute(attributeName,null,getNumericBins(values)));
            } else { //add nominal attribute to table as default
                //TODO what to do if nothing is common enough to be an important value?
                t.addAttribute(new NominalAttribute(attributeName,null,getNominalBins(values,numRows)));
            }
        }
    }

    public ArrayList<String> getNominalBins(HashMap<String, Integer> values, int numRows) {
        ArrayList<String> nominalBins = new ArrayList<>();
        for(String value: values.keySet()) {
            if(values.get(value)>(double)numRows*0.00d) {
                nominalBins.add(value);
            }
        }
        return nominalBins;
    }

    public ArrayList<Double> getNumericBins(HashMap<String, Integer> values) {
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
        return numericBins;
    }

    public boolean isUnique(String attName, Table t) throws Exception {
        ResultSet rs = conn.query(conn.buildSQLToGetDifferenceBetweenTotalAndNumUnique(attName, t));
        int diff = Integer.parseInt(rs.getString(1));
        return diff==0 ? true : false;
    }

    private boolean tableContainsAttribute(Table t, String attName) {
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

    private String encloseInBrackets(String s) {
        return "["+s+"]";
    }

}
