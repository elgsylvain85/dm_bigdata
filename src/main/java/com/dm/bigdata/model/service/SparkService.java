package com.dm.bigdata.model.service;

import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
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
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.dm.bigdata.model.dao.TableImportedDao;
import com.dm.bigdata.model.pojo.TableImported;

import scala.Function1;

@Service
public class SparkService {

    static final Logger LOGGER = Logger.getLogger(SparkService.class.getName());
    static final String SPARK_DATABASE = "default";
    static final String SPARK_GLOBAL_TEMP = "global_temp";
    static final String ITEM_SEPARATOR = "\n";
    public static final String[] TABLES_STATUS_HEADER = { "File", "Quantity", "Active" };
    private static final String DATA_KEY = "data";
    private static final String TOTAL_COUNT_KEY = "totalCount";
    private static final String FILE_STRUCTURE_KEY = "fileStructure";
    private static final String FILE_PREVIEW_KEY = "filePreview";
    static final String JOIN_TABLE_NAME = "joindata";

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

                } else if (joinValue != null) {
                    cellule = joinValue;

                } else {
                    /* avoid null value */
                    cellule = "";
                }

                // if (cellule != null) {
                rowValues[i] = cellule;
                // }
            }
            return RowFactory.create(rowValues);
        }
    }

    static Map<String, String[]> dataStatus = new HashMap<String, String[]>();

    @Autowired
    transient SparkSession sparkSession;

    @Autowired
    transient FileSystem hadoopFileSystem;

    @Autowired
    transient AppColumnService appColumnService;

    @Autowired
    transient TableImportedDao tableImportedDao;

    @Value("${app.work-dir}")
    transient String appWorkDir;

    @Value("${app.hadoop-namenode}")
    transient String hadoopNameNode;

    @Value("${app.spark-join-partitions}")
    transient String appSparkJoinPartitions;

    public Map<String, Object> dataAsMap(
            int offset,
            int limit,
            String filterExpression)
            throws Exception {

        Map<String, Object> result = new HashMap<String, Object>();

        var tablesNames = this.localTablesNames();

        if (tablesNames.length > 0) {

            var fullTableName = SPARK_DATABASE + "." + SparkService.JOIN_TABLE_NAME;
            Dataset<Row> ds = this.sparkSession.sqlContext().table(fullTableName);
            var columnsAsString = this.appColumnService.columnsWithSource();

            var columns = columnsAsString.stream().map(new Function<String, Column>() {

                @Override
                public Column apply(String t) {
                    return new Column(t);
                }

            }).toArray(Column[]::new);

            ds = ds.select(columns);

            if (filterExpression != null) {
                ds = ds.where(filterExpression);
            }

            // var totalCount = ds.count();

            var data = ds.limit(limit).toJSON().collectAsList();

            result.put(SparkService.FILE_STRUCTURE_KEY, columnsAsString);
            result.put(SparkService.DATA_KEY, data);
            // result.put(SparkService.TOTAL_COUNT_KEY, totalCount);
            result.put(SparkService.TOTAL_COUNT_KEY, "-");

        }

        return result;
    }

    public void exportResult(
            String filterExpression)
            throws Exception {

        var fileName = "export_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                + ".csv";

        try {

            var exportFolder = new File(this.appWorkDir + "/export");

            /* if exportFolder not exist then create that */

            if (!exportFolder.exists()) {
                exportFolder.mkdir();
            }

            SparkService.dataStatus.put(fileName,
                    new String[] { fileName, "-", "EXPORTING", });

            var tablesNames = this.localTablesNames();

            if (tablesNames.length > 0) {

                var fullTableName = SPARK_DATABASE + "." + SparkService.JOIN_TABLE_NAME;
                Dataset<Row> ds = this.sparkSession.sqlContext().table(fullTableName);
                var columnsAsString = this.appColumnService.columnsWithSource();

                var columns = columnsAsString.stream().map(new Function<String, Column>() {

                    @Override
                    public Column apply(String t) {
                        return new Column(t);
                    }

                }).toArray(Column[]::new);

                ds = ds.select(columns);

                if (filterExpression != null) {
                    ds = ds.where(filterExpression);
                }

                // var totalCount = ds.count();

                var path = exportFolder.getAbsolutePath() + File.separator + fileName;

                ds.coalesce(1).write().option("header", true).csv(path);

                // SparkService.dataStatus.put(fileName,
                // new String[] { fileName, String.valueOf(totalCount), "EXPORTED", });
                SparkService.dataStatus.put(fileName,
                        new String[] { fileName, "-", "EXPORTED", });

            } else {
                throw new Exception("Error export : No tables already imported");
            }

        } catch (Exception ex) {

            /* set error status */

            SparkService.dataStatus.put(fileName,
                    new String[] { fileName, "-", "EXPORT ERROR", });

            throw ex;
        }
    }

    public String[] localTablesNames() {

        return this.sparkSession.sqlContext().tableNames(SPARK_DATABASE);
    }

    private synchronized static void processImportFile(SparkService instance, String path, String sourceName,
            Map<String, String> columnsMapping,
            String delimiter,
            boolean excludeHeader, List<String> joinColumns) throws Exception {

        SparkService.dataStatus.put(sourceName,
                new String[] { sourceName, "-", "PREPARING", });

        var dataFrameReader = instance.sparkSession.read();

        /* Import data */

        var dataImport = dataFrameReader.option("delimiter", delimiter)
                .option("header", excludeHeader).csv(instance.hadoopNameNode + path);

        /*
         * number each temp file column name to avoid confusion between real and
         * temporary columns
         */

        var dataImportCols = dataImport.columns();
        Map<String, String> newColumnsMapping = new HashMap<>();

        for (int i = 0; i < dataImportCols.length; i++) {
            var tempCol = dataImportCols[i] + i + "temp";

            // rename column in file to import
            dataImport = dataImport.withColumnRenamed(dataImportCols[i], tempCol);

            // rename too in file structure column mapping
            newColumnsMapping.put(tempCol, columnsMapping.get(dataImportCols[i]));

        }

        /* now rename columns according mapping */

        var appColumnsNoMapped = instance.appColumnService.columnsToUpperCase();// to know which columns not used and
                                                                                // then complete
        // with
        // empty
        // value

        for (var k : newColumnsMapping.keySet()) {
            String columnMapped = newColumnsMapping.get(k);

            if (columnMapped != null) {

                columnMapped = columnMapped.replaceAll("[^a-zA-Z0-9]", "");// remove all special char
                columnMapped = columnMapped.toUpperCase();// use facilite comparaison
                /* rename with column mapped */
                dataImport = dataImport.withColumnRenamed(k, columnMapped);

                /* if column mapped not exist in system then add as new app columns */

                if (!appColumnsNoMapped.contains(columnMapped)) {
                    instance.updateColumnByName(null, columnMapped);
                } else {
                    // else just remove from list after mapping
                    appColumnsNoMapped.remove(columnMapped);
                }
            } else {
                /* null means column must be ignored */
                dataImport = dataImport.drop(k);
            }
        }

        /* complete all app columns not used with null value */

        for (var c : appColumnsNoMapped) {
            dataImport = dataImport.withColumn(c, functions.lit(""));
        }

        /* add "sources" new column with tablename as value -> needed when join */

        dataImport = dataImport.withColumn(AppColumnService.SOURCE_COLUMN, functions.lit(sourceName));

        /* start import from file */

        SparkService.dataStatus.put(sourceName,
                new String[] { sourceName, "-", "IMPORTING", });

        /* load all columns after file prepared */

        var columnsAsString = instance.appColumnService.columnsWithSource();
        var columns = columnsAsString.stream().map(new Function<String, Column>() {

            @Override
            public Column apply(String t) {
                return new Column(t);
            }

        }).toArray(Column[]::new);

        /* align data according app columns order */

        dataImport = dataImport.select(columns);

        /* group by columns to remove doublon */

        dataImport = dataImport.groupBy(columns).df();

        var dataImportCount = dataImport.count();

        /* persist operation */

        SparkService.dataStatus.put(sourceName,
                new String[] { sourceName, String.valueOf(dataImportCount), "SAVING", });

        // var joinColumnsAsList = instance.appColumnService.joins();

        /*
         * join only if preview data exist and join column is not empty else append or
         * save directly
         */

        var dataJoinExist = Arrays.asList(instance.localTablesNames()).contains(SparkService.JOIN_TABLE_NAME);

        if (dataJoinExist) {

            if (joinColumns != null && !joinColumns.isEmpty()) {

                /* join if join data already exists and joinColumns is not empty */

                /* save import data as table temp table before join */

                dataImport.createOrReplaceTempView(sourceName);
                var tempTable = instance.sparkSession.sqlContext().table(sourceName).cache();

                SparkService.dataStatus.put(sourceName,
                        new String[] { sourceName, String.valueOf(dataImportCount),
                                "JOINING ON " + joinColumns, });

                /* apply join from existant data with temp table (from data imported) */

                var dataJoinTableName = SPARK_DATABASE + "." + SparkService.JOIN_TABLE_NAME;
                var dataJoin = instance.sparkSession.sqlContext().table(dataJoinTableName);

                dataJoin = instance.joinWithMapProcess(dataJoin, tempTable, columns, joinColumns);

                /* then overwrite result of jointure as base */

                SparkService.dataStatus.put(sourceName,
                        new String[] { sourceName, String.valueOf(dataImportCount),
                                "JOIN APPENDING ON " + joinColumns, });

                dataJoin.repartition(Integer.valueOf(instance.appSparkJoinPartitions)).write()
                        .format("delta")
                        .mode(SaveMode.Overwrite)
                        .option("overwriteSchema", "true")
                        .saveAsTable(SparkService.JOIN_TABLE_NAME);

                /* free memory */
                try {
                    // dataJoin.unpersist(true);
                    instance.sparkSession.catalog().uncacheTable(sourceName);
                    instance.sparkSession.catalog().dropTempView(sourceName);
                    // dataImport.unpersist(true);

                } catch (Exception ex) {
                    SparkService.LOGGER.log(Level.WARNING, "free memory after join failed", ex);
                }

            } else {

                /* append if join data already exists and joinColumns is empty */

                SparkService.dataStatus.put(sourceName,
                        new String[] { sourceName, String.valueOf(dataImportCount), "APPENDING", });

                /* write data imported directly as base */

                dataImport.write()
                        .format("delta")
                        .mode(SaveMode.Append)
                        .option("overwriteSchema", "true")
                        .saveAsTable(SparkService.JOIN_TABLE_NAME);

                dataImport.unpersist(true);
            }

        } else {

            /* overwriting data nothing exists */

            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, String.valueOf(dataImportCount), "OVERWRITING", });

            /* write data imported directly as base */

            dataImport.write()
                    .format("delta")
                    .mode(SaveMode.Overwrite)
                    .option("overwriteSchema", "true")
                    .saveAsTable(SparkService.JOIN_TABLE_NAME);

            dataImport.unpersist(true);

        }

        // /* cache data */
        // this.cacheFullData();

        /* remove importing status */
        SparkService.dataStatus.remove(sourceName);

        /* update file import log */

        try {

            var entity = instance.tableImportedDao.findByTableName(sourceName);
            if (entity == null) {
                entity = new TableImported(sourceName);
            }

            entity.setRowsCount(BigDecimal.valueOf(dataImportCount).add(entity.getRowsCount()));

            instance.tableImportedDao.save(entity);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Update file import log Error", ex);
        } finally {
            /* delete tenmp file from hadoop */

            var hadoopPath = new org.apache.hadoop.fs.Path(instance.hadoopNameNode + path);

            instance.hadoopFileSystem.delete(hadoopPath, true);
        }

    }

    private Dataset<Row> joinWithMapProcess(Dataset<Row> dataJoin, Dataset<Row> dataImport, Column[] columns,
            List<String> joinColumnsAsList) {

        /*
         * align two dataframe to synchronized map
         * function
         */

        dataImport = dataImport.select(columns);
        dataJoin = dataJoin.select(columns);

        var joinColumns = joinColumnsAsList.toArray(new String[] {});

        /* prepare join expr */

        Column colJoinExprs = null;
        for (int j = 0; j < joinColumns.length; j++) {

            var colName = joinColumns[j];

            var colExpr = dataImport.col(colName).equalTo(dataJoin.col(colName));

            if (j == 0) {
                colJoinExprs = colExpr;
            } else {
                // use OR SQL expression
                colJoinExprs = colJoinExprs.or(colExpr);
            }
        }

        /* apply join */

        if (colJoinExprs != null) {
            dataJoin = dataJoin.join(dataImport, colJoinExprs, "full");
        } else {
            dataJoin = dataJoin.join(dataImport);
        }

        /* merge columns to avoid ambigues column */

        dataJoin = dataJoin.repartition(Integer.valueOf(this.appSparkJoinPartitions))
                .map(new SparkService.JoinFunction(), dataImport.encoder()).select(columns);

        return dataJoin;
    }

    public void prepareImportFile(String path, String sourceName, Map<String, String> columnsMapping,
            String delimiter,
            boolean excludeHeader, List<String> joinColumns) throws Exception {

        try {

            /* preparing file to import */

            /* Get file ou directory name to use as table [source] name */

            sourceName = sourceName.replaceAll("[^a-zA-Z0-9]", "");// remove all special char

            /* add informations according [TABLES_STATUS_HEADER] */

            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, "-", "QUEUE", });

            SparkService.processImportFile(this, path, sourceName, columnsMapping, delimiter, excludeHeader,
                    joinColumns);

        } catch (Exception ex) {

            /* set error status */

            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, ex.getMessage(), "IMPORT ERROR", });

            throw ex;
        }
    }

    public void dropTable(String tableName) throws Exception {

        if (tableName != null) {
            this.sparkSession.sql("DROP TABLE IF EXISTS " + SparkService.SPARK_DATABASE + "." + tableName + ";");

            var entity = this.tableImportedDao.findByTableName(tableName);

            if (entity != null) {
                this.tableImportedDao.delete(entity);
            }

        } else {
            /* clear all tables if [tableName] in parameters is null */

            for (var e : this.localTablesNames()) {
                this.dropTable(e);
            }

            this.tableImportedDao.deleteAll();
        }

    }

    /**
     * Load data from all files loaded indicated in parameter, auto generated query
     * with join if necessary
     * 
     * @param tablesNames
     * @param joinColumns
     * @return
     * @throws Exception
     */
    private synchronized Dataset<Row> joinWithQueryProcess(String[] tablesNames, List<String> columns,
            List<String> joinColumns)
            throws Exception {

        if (tablesNames.length > 0) {

            /* SELECT composition with columns (concatenation) */

            var query = " SELECT ";
            for (int i = 0; i < columns.size(); i++) {
                var col = "";

                if (tablesNames.length == 1) {
                    /* get unique column */
                    col = columns.get(i);
                } else {
                    /* Concatenate columns from many tables with system separator */
                    col = " CONCAT_WS(\"" + SparkService.ITEM_SEPARATOR + "\", ";
                    for (int j = 0; j < tablesNames.length; j++) {
                        col += tablesNames[j] + "." + columns.get(i);

                        if (j != tablesNames.length - 1) {
                            col += " , ";
                        }
                    }
                    col += " ) AS " + columns.get(i);
                }

                /* add column to select */

                query += col;

                if (i != columns.size() - 1) {
                    query += " , ";
                }
            }

            /* FROM and JOIN composition */

            query += " FROM ";

            for (int i = 0; i < tablesNames.length; i++) {
                if (i == 0) {
                    /* first tables just table without join */
                    query += tablesNames[i];
                } else {
                    /* from second table then apply join */

                    if (joinColumns.isEmpty()) {
                        /* if no join columns has been indicated then apply natural join */
                        query += " CROSS JOIN " + tablesNames[i];
                    } else {
                        /* apply full just with preview table on specified columns */
                        query += " FULL JOIN " + tablesNames[i];

                        for (int j = 0; j < joinColumns.size(); j++) {
                            if (j == 0) {
                                query += " ON ";
                            } else {
                                query += " OR ";
                            }

                            /* i-1 to indicate preview table */
                            query += tablesNames[i - 1] + "." + joinColumns.get(j) + " == " + tablesNames[i] + "."
                                    + joinColumns.get(j);
                        }
                    }
                }
            }

            LOGGER.info(query);

            var data = this.sparkSession.sqlContext().sql(query);

            return data;

        } else {
            throw new Exception("No table available");
        }

    }

    /**
     * Return Structure of file and (x) data preview
     * 
     * @param filePath
     * @param delimiter
     * @return
     * @throws Exception
     */
    public Map<String, Object> fileStructure(String filePath, String delimiter, boolean excludeHeader, int limit)
            throws Exception {
        var df = this.sparkSession.read()
                .option("delimiter", delimiter).option("header", excludeHeader)
                .csv(this.hadoopNameNode + filePath);

        Map<String, Object> result = new HashMap<String, Object>();

        var data = df.limit(limit).toJSON().collectAsList();
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
        newColumnName = newColumnName.toUpperCase();

        for (var tableName : this.localTablesNames()) {

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

        columnName = columnName.toUpperCase();

        for (var tableName : this.localTablesNames()) {

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

        /* all importing tables */

        for (var k : SparkService.dataStatus.keySet()) {
            result.add(SparkService.dataStatus.get(k));
        }

        /* tables imported log */

        for (var e : this.tableImportedDao.findAll()) {

            result.add(new String[] { e.getTableName(),
                    String.valueOf(e.getRowsCount().longValue()), "DONE", });

        }

        // /* all existants tables */

        // for (var tableName : this.localTablesNames()) {

        // var fullTableName = SPARK_DATABASE + "." + tableName;

        // var df = this.sparkSession.sqlContext().table(fullTableName);

        // var count = df.count();

        // /* add informations according [TABLES_STATUS_HEADER] */

        // result.add(new String[] { tableName, String.valueOf(count), "DONE", });

        // }
        /* Join current information */

        var fullTableName = SPARK_DATABASE + "." + JOIN_TABLE_NAME;

        var joinTableExist = this.sparkSession.catalog().tableExists(fullTableName);

        long count = 0;

        if (joinTableExist) {
            var df = this.sparkSession.sqlContext().table(fullTableName);

            count = df.count();

            result.add(new String[] { JOIN_TABLE_NAME, String.valueOf(count), "READY", });
        } else {

            result.add(new String[] { JOIN_TABLE_NAME, String.valueOf(count), "EMPTY", });
        }

        return result;
    }

    // public File[] filesToImport() {

    // var wf = new File(this.appWorkDir);

    // /* file worker folder not exist then create that */

    // if (!wf.exists()) {
    // wf.mkdir();
    // }

    // /* add files to exclude */

    // var exclused = new ArrayList<File>();

    // // exclude hide and system files

    // exclused.addAll(
    // Arrays.asList(
    // wf.listFiles(
    // new FileFilter() {

    // public boolean accept(File file) {

    // return file.isHidden();
    // }
    // })));

    // /* apply exclude filter on working folder */

    // var result =

    // wf.listFiles(new FileFilter() {

    // public boolean accept(File file) {
    // return !exclused.contains(file);
    // }
    // });

    // return result;
    // }

    // public List<String> filesToImportAsPath() {
    // var result = new ArrayList<String>();

    // for (var f : this.filesToImport()) {
    // result.add(f.getAbsolutePath());
    // }

    // return result;
    // }

    public void updateJoin(String columnName, Boolean value) throws Exception {
        this.appColumnService.updateJoin(columnName, value);

    }

    public List<TableImported> tablesImported() {
        return this.tableImportedDao.findAll();
    }

    public String uploadFile(byte[] inputStream, String name) throws IOException {

        /* write input stream in local system as temp file */

        var tempFile = Files.createTempFile(name, null);

        var outputStream = new FileOutputStream(tempFile.toFile());

        outputStream.write(inputStream);
        outputStream.close();

        /* check and/or create import dir in hadoop */

        var importDirPath = new org.apache.hadoop.fs.Path(this.appWorkDir + "/import");

        var exist = this.hadoopFileSystem.exists(importDirPath);

        if (!exist) {
            this.hadoopFileSystem.mkdirs(importDirPath);
        }

        /* move temp file to hadoop */

        var from = new org.apache.hadoop.fs.Path(tempFile.toString());
        var to = new org.apache.hadoop.fs.Path(importDirPath.toString() + "/" + tempFile.toFile().getName());

        this.hadoopFileSystem.moveFromLocalFile(from, to);

        /* delete temp file from local system */
        tempFile.toFile().delete();

        /* return path of destination file in hadoop */

        return to.toString();
    }

    // @EventListener(ApplicationReadyEvent.class)
    // public void cacheFullData() {

    // try {

    // var tablesNames = this.localTablesNames();
    // var columns = this.appColumnService.columnsWithSource();
    // var joinColumns = this.appColumnService.joins();

    // if (tablesNames.length > 0) {

    // this.queryData(tablesNames, columns,
    // joinColumns).cache();

    // } else {
    // throw new Exception("No tables already imported");
    // }

    // } catch (Exception ex) {

    // LOGGER.log(Level.SEVERE, "Cache full data error", ex);

    // }
    // }
}
