package com.dm.bigdata.model.service;

import java.io.File;
import java.io.FileFilter;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import scala.Function1;

@Service
public class SparkService {

    // private static final long serialVersionUID = 1L;

    static final Logger LOGGER = Logger.getLogger(SparkService.class.getName());
    static final String SPARK_DATABASE = "default";
    static final String ITEM_SEPARATOR = "\n";
    public static final String[] TABLES_STATUS_HEADER = { "File", "Quantity", "Active" };
    private static final String DATA_KEY = "data";
    private static final String TOTAL_COUNT_KEY = "totalCount";
    private static final String FILE_STRUCTURE_KEY = "fileStructure";
    private static final String FILE_PREVIEW_KEY = "filePreview";

    public static class JoinFunction implements MapFunction<Row, Row> {

        @Override
        public Row call(Row row) {

            final int columnsCount = row.length() / 2;// because using join of two table

            var rowValues = new Object[columnsCount];

            for (int i = 0; i < columnsCount; i++) {

                /*
                 * The columns are even in row and ordered according to the tables. Ex. :
                 * col1,
                 * col2, col3, col1, clo2, col3
                 */
                int p = i + columnsCount;

                String cellule = null;

                String leftValue = row.getString(i);
                String joinValue = row.getString(p);

                if (leftValue != null) {

                    /*
                     * if the join column contain same value or exist value then take just first
                     * else merge both
                     */

                    var exist = false;

                    for (var v : leftValue.split(ITEM_SEPARATOR)) {

                        if (v.equalsIgnoreCase(joinValue)) {
                            exist = true;
                            break;
                        }
                    }

                    if (exist) {
                        cellule = leftValue;
                    } else {
                        cellule = leftValue;

                        if (joinValue != null) {
                            cellule += ITEM_SEPARATOR + joinValue;
                        }
                    }

                } else {
                    if (joinValue != null) {
                        cellule = joinValue;
                    }
                }

                if (cellule != null) {
                    rowValues[i] = cellule;

                }
            }
            return RowFactory.create(rowValues);
        }

    }

    static Map<String, String[]> importingTablesStatus = new HashMap<String, String[]>();

    @Autowired
    transient SparkSession sparkSession;

    @Autowired
    transient AppColumnService appColumnService;

    @Value("${app.worker-folder}")
    transient String workFolder;

    public Map<String, Object> data(
            // String tableName,
            int offset,
            int limit,
            String filterExpression)
            throws Exception {

        // List<String> result;
        Map<String, Object> result = new HashMap<String, Object>();

        Dataset<Row> ds = null;

        var columns = this.appColumnService.columnsWithSource();
        var joinColumns = this.appColumnService.joins();

        var sparkColumns = new ArrayList<Column>();
        if (sparkColumns != null) {
            for (var c : columns) {
                sparkColumns.add(new Column(c));
            }
        }

        // /* if file name is null then call fileData without file name arg */

        var tablesNames = this.tablesNames();

        ds = this.data(tablesNames, sparkColumns.toArray(new Column[] {}),
                joinColumns.toArray(new String[] {}));

        try {

            /* apply filtre if exist */

            if (filterExpression != null) {
                ds = ds.filter(filterExpression);
            }

            /* get result total Count before apply limit */

            ds = ds.cache();
            var totalCount = ds.count();

            /* apply limit */

            ds = ds.limit(limit);

            var data = ds.toJSON().collectAsList();

            result.put(SparkService.DATA_KEY, data);
            result.put(SparkService.TOTAL_COUNT_KEY, totalCount);

            return result;
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, ex.getMessage(), ex);

            /* return empty data if error */

            // return this.sparkSession.emptyDataFrame().toJSON().collectAsList();
            result.put("data", new ArrayList<String>());
            result.put("totalCount", 0);

            return result;

        }
    }

    public String[] tablesNames() {

        return this.sparkSession.sqlContext().tableNames(SPARK_DATABASE);
    }

    public void importFile(String path, String tableName, Map<String, String> fileStructure,
            String delimiter,
            boolean excludeHeader) throws Exception {

        try {

            /* Get file ou directory name to use as table [source] name */

            tableName = tableName.replaceAll("[^a-zA-Z0-9]", "");// remove all special char

            /* add informations according [TABLES_STATUS_HEADER] */

            SparkService.importingTablesStatus.put(tableName,
                    new String[] { tableName, "-", "PREPARING", });

            var dataFrameReader = this.sparkSession.read();

            /* Import data */

            var data = dataFrameReader.option("delimiter", delimiter)
                    .option("header", excludeHeader).csv(path);

            /* rename columns according mapping */

            var tempAppColumns = this.appColumnService.columns();// to know which columns not used and complete with
                                                                 // null
                                                                 // value
            for (var k : fileStructure.keySet()) {
                String columnMapped = fileStructure.get(k);

                if (columnMapped != null) {

                    columnMapped = columnMapped.replaceAll("[^a-zA-Z0-9]", "");// remove all special char

                    /* rename with column mapped */
                    data = data.withColumnRenamed(k, columnMapped);

                    /* if column mapped not exist then add as new app columns */

                    if (!tempAppColumns.contains(columnMapped)) {
                        // this.appColumnService.updateColumn(null, columnMapped);
                        this.updateColumnByName(null, columnMapped);
                    } else {
                        tempAppColumns.remove(columnMapped);// already used
                    }
                } else {
                    /* null means column must be ignored */
                    data = data.drop(k);
                }
            }

            /* complete all app columns not used with null value */

            for (var c : tempAppColumns) {
                data = data.withColumn(c, functions.lit(""));
            }

            /* add "sources" new column with tablename as value -> needed when join */

            data = data.withColumn(AppColumnService.SOURCE_COLUMN, functions.lit(tableName));

            /* persist data : append if table already exist else overwrite */

            /* update status */

            var count = data.count();

            SparkService.importingTablesStatus.put(tableName,
                    new String[] { tableName, String.valueOf(count), "IMPORTING", });

            if (Arrays.asList(this.tablesNames()).contains(tableName)) {

                SparkService.importingTablesStatus.put(tableName,
                        new String[] { tableName, "-", "APPEND", });

                data.write()
                        .format("delta")
                        .mode(SaveMode.Append)
                        .option("overwriteSchema", "true")
                        .saveAsTable(tableName);
            } else {
                data.write()
                        .format("delta")
                        .mode(SaveMode.Overwrite)
                        .option("overwriteSchema", "true")
                        .saveAsTable(tableName);
            }

            /* remove importing status */
            SparkService.importingTablesStatus.remove(tableName);

        } catch (Exception ex) {

            /* set error status */

            SparkService.importingTablesStatus.put(tableName,
                    new String[] { tableName, "-", "ERROR", });

            throw ex;

        }
        // finally {
        // /* remove importing status */
        // SparkService.importingTablesStatus.remove(tableName);
        // }

        // this.sparkSession.catalog().listTables().show();
    }

    public void dropTable(String tableName) {
        this.sparkSession.sql("DROP TABLE IF EXISTS " + SparkService.SPARK_DATABASE + "." + tableName + ";");

        this.sparkSession.catalog().listTables().show();
    }

    /**
     * Load data from all files loaded indicated in parameter and apply join
     * 
     * @param tablesNames
     * @param joinColumns
     * @return
     * @throws Exception
     */
    private Dataset<Row> data(String[] tablesNames, Column[] columns,
            String[] joinColumns)
            throws Exception {

        Dataset<Row> ds = this.sparkSession.emptyDataFrame();
        Dataset<Row> old_ds = this.sparkSession.emptyDataFrame();

        for (int i = 0; i < tablesNames.length; i++) {
            String fileNameLoaded = tablesNames[i];

            ds = this.data(fileNameLoaded, columns);

            if (i == 0) {
                old_ds = ds;
            } else {

                /* prepare join expr */

                Column colJoinExprs = null;
                for (int j = 0; j < joinColumns.length; j++) {

                    var colName = joinColumns[j];

                    var colExpr = ds.col(colName).equalTo(old_ds.col(colName));

                    if (j == 0) {
                        colJoinExprs = colExpr;
                    } else {
                        // use AND SQL expression
                        colJoinExprs = colJoinExprs.and(colExpr);
                    }
                }

                /* apply join */

                if (colJoinExprs != null) {
                    old_ds = old_ds.join(ds, colJoinExprs, "full");
                } else {
                    old_ds = old_ds.join(ds);
                }

                /* merge columns to avoid ambigues column */

                old_ds = old_ds.map(new SparkService.JoinFunction(), ds.encoder()).select(columns);
            }
        }
        return old_ds;
    }

    /**
     * Load file loaded
     * 
     * @param tableName
     * @return
     * @throws Exception
     */
    private Dataset<Row> data(String tableName, Column[] columns)
            throws Exception {

        var table = SPARK_DATABASE + "." + tableName;

        var data = this.sparkSession.sqlContext().table(table).select(columns);

        return data;

    }

    /**
     * Return Structure of file and (x) data preview
     * 
     * @param filePath
     * @param delimiter
     * @return
     * @throws Exception
     */
    public Map<String, Object> fileStructure(String filePath, String delimiter, boolean excludeHeader) throws Exception {
        var df = this.sparkSession.read()
                .option("delimiter", delimiter).option("header", excludeHeader)
                .csv(filePath);

        Map<String, Object> result = new HashMap<String, Object>();

        var data = df.limit(5).toJSON().collectAsList();
        var structure = df.schema().fieldNames();

        result.put(SparkService.FILE_STRUCTURE_KEY, structure);
        result.put(SparkService.FILE_PREVIEW_KEY, data);

        return result;
    }

    /**
     * Foreach all exist tables to update column information and update default app
     * column
     * 
     * @param oldColumnName
     * @param newColumnName
     * @throws NoSuchTableException
     */
    public void updateColumnByName(String oldColumnName, String newColumnName) throws Exception {

        newColumnName = newColumnName.replaceAll("[^a-zA-Z0-9]", "");// remove all special char

        for (var tableName : this.tablesNames()) {

            var fullTableName = SPARK_DATABASE + "." + tableName;

            var df = this.sparkSession.sqlContext().table(fullTableName);

            /* if table containt old column then rename */

            if (Arrays.asList(df.columns()).contains(oldColumnName)) {
                this.sparkSession.sqlContext().sql(
                        "ALTER TABLE " + fullTableName + " RENAME COLUMN " + oldColumnName + " TO " + newColumnName);

            } else {
                /* else add new column to table with empty value */

                this.sparkSession.sqlContext().sql(
                        "ALTER TABLE " + fullTableName + " ADD COLUMNS (" + newColumnName + " STRING)");
            }

        }

        this.appColumnService.updateColumnByName(oldColumnName, newColumnName);
    }

    /**
     * Foreach all exist tables to delete column information and delete default app
     * column
     * 
     * @param columnName
     * @throws NoSuchTableException
     */
    public void deleteColumnByName(String columnName) throws Exception {

        for (var tableName : this.tablesNames()) {

            var fullTableName = SPARK_DATABASE + "." + tableName;

            var df = this.sparkSession.sqlContext().table(fullTableName);

            /* if table containt column then remove */

            if (Arrays.asList(df.columns()).contains(columnName)) {
                this.sparkSession.sqlContext().sql(
                        "ALTER TABLE " + fullTableName + " DROP COLUMN " + columnName);

            }

        }

        this.appColumnService.deleteColumnByName(columnName);
    }

    public List<String[]> tablesStatus() {
        var result = new ArrayList<String[]>();

        // /* header */

        // result.add(SparkService.TABLES_STATUS_HEADER);

        /* all existants tables */

        for (var tableName : this.tablesNames()) {

            var fullTableName = SPARK_DATABASE + "." + tableName;

            var df = this.sparkSession.sqlContext().table(fullTableName);

            var count = df.count();

            /* add informations according [TABLES_STATUS_HEADER] */

            result.add(new String[] { tableName, String.valueOf(count), "DONE", });

        }

        /* all importing tables */

        for (var k : SparkService.importingTablesStatus.keySet()) {
            result.add(SparkService.importingTablesStatus.get(k));
        }

        return result;
    }

    public File[] filesToImport() {

        var wf = new File(this.workFolder);

        /* file worker folder not exist then create that */

        if (!wf.exists()) {
            wf.mkdir();
        }

        /* add files to exclude */

        var exclused = new ArrayList<File>();

        // exclude hide and system files

        exclused.addAll(
                Arrays.asList(
                        wf.listFiles(
                                new FileFilter() {

                                    public boolean accept(File file) {

                                        return file.isHidden();
                                    }
                                })));

        /* apply exclude filter on working folder */

        var result =

                wf.listFiles(new FileFilter() {

                    public boolean accept(File file) {
                        return !exclused.contains(file);
                    }
                });

        return result;
    }

    public List<String> filesToImportAsPath() {
        var result = new ArrayList<String>();

        for (var f : this.filesToImport()) {
            result.add(f.getAbsolutePath());
        }

        return result;
    }
}
