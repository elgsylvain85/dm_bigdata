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
import java.sql.Connection;
import java.sql.SQLException;
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

import com.dm.bigdata.model.dao.AppInfoDao;
import com.dm.bigdata.model.dao.TableImportedDao;
import com.dm.bigdata.model.pojo.AppInfo;
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
    static final int VARCHAR_COLUMN_SIZE = 255 * 100;

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

    // @Autowired
    // transient FileSystem hadoopFileSystem;

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

    @Value("${spring.datasource.url}")
    transient String springDatasourceUrl;
    @Value("${spring.datasource.username}")
    transient String springDatasourceUsername;
    @Value("${spring.datasource.password}")
    transient String springDatasourcePassword;

    @Autowired
    transient Connection jdbc;

    @Autowired
    transient AppInfoDao appInfoDao;

    public Map<String, Object> dataAsMap(
            int offset,
            int limit,
            String filterExpression)
            throws Exception {

        Map<String, Object> result = new HashMap<String, Object>();

        var columns = this.appColumnService.columnsWithSource().toArray(String[]::new);

        var joinColumns = this.appColumnService.joins().toArray(String[]::new);

        Dataset<Row> ds = this.queryBySQL(new String[] { SparkService.JOIN_TABLE_NAME }, columns, joinColumns, limit,
                filterExpression);

        // ds = ds.selectExpr(columns);

        var data = ds.toJSON().collectAsList();

        result.put(SparkService.FILE_STRUCTURE_KEY, columns);
        result.put(SparkService.DATA_KEY, data);
        result.put(SparkService.TOTAL_COUNT_KEY, "-");

        return result;
    }

    public void exportResult(
            String filterExpression)
            throws Exception {

        var fileName = "export_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                + ".csv";

        try {

            var exportFolder = new File(this.appWorkDir + File.separatorChar + "temp" + File.separatorChar + "export");

            /* if exportFolder not exist then create that */

            if (!exportFolder.exists()) {
                exportFolder.mkdirs();
            }

            SparkService.dataStatus.put(fileName,
                    new String[] { fileName, "-", "EXPORTING", });

            var columns = this.appColumnService.columnsWithSource().toArray(String[]::new);

            var joinColumns = this.appColumnService.joins().toArray(String[]::new);

            Dataset<Row> ds = this.queryBySQL(new String[] { SparkService.JOIN_TABLE_NAME }, columns, joinColumns,
                    -1,
                    filterExpression);

            // var totalCount = ds.count();

            var path = exportFolder.getAbsolutePath() + File.separator + fileName;

            ds.coalesce(1).write().option("header", true).csv(path);

            SparkService.dataStatus.put(fileName,
                    new String[] { fileName, "-", "EXPORTED", });

        } catch (Exception ex) {

            /* set error status */

            SparkService.dataStatus.put(fileName,
                    new String[] { fileName, "-", "EXPORT ERROR", });

            throw ex;
        }
    }

    public String[] allSources() {

        var tablesImported = this.tableImportedDao.findAll().stream().map((e) -> e.getTableName())
                .collect(Collectors.toList());

        return tablesImported.toArray(String[]::new);
    }

    private synchronized static void processImportFile(SparkService instance, String path, String sourceName,
            Map<String, String> columnsMapping,
            String delimiter,
            boolean excludeHeader) throws Exception {

        SparkService.dataStatus.put(sourceName,
                new String[] { sourceName, "-", "PREPARING", });

                var fileSize = Paths.get(path).toFile().length();
                var partition = (int)(fileSize / 200000000);//200 Mb per partition

                if(partition < 1){
                    partition = 1;
                }

        var dataFrameReader = instance.sparkSession.read();

        /* Import data */

        var dataImport = dataFrameReader.option("delimiter", delimiter)
                .option("header", excludeHeader).csv(path);

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

        // var dataImportCount = dataImport.count();

        /* persist operation */

        // SparkService.dataStatus.put(sourceName,
        // new String[] { sourceName, String.valueOf(dataImportCount), "SAVING", });
        SparkService.dataStatus.put(sourceName,
                new String[] { sourceName, "-", "SAVING", });

        /*
         * import only if preview data exist and else
         * save directly
         */

        var tableExist = Arrays.asList(instance.allSources()).contains(sourceName);

        if (tableExist) {

            // SparkService.dataStatus.put(sourceName,
            // new String[] { sourceName, String.valueOf(dataImportCount), "APPENDING", });
            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, "-", "APPENDING", });

            /* Append data imported with existant table */

            dataImport.repartition(partition).write()
                    .format("jdbc")
                    .mode(SaveMode.Append)
                    .option("overwriteSchema", "true")
                    .option("url", instance.springDatasourceUrl)
                    .option("dbtable", sourceName)// "dbo.Employees2"
                    .option("user", instance.springDatasourceUsername)
                    .option("password", instance.springDatasourcePassword)
                    .save();

            dataImport.unpersist(true);

        } else {

            /* overwriting data nothing exists */

            // SparkService.dataStatus.put(sourceName,
            // new String[] { sourceName, String.valueOf(dataImportCount), "OVERWRITING",
            // });
            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, "-", "OVERWRITING", });

            /* write data imported directly as base */

            dataImport.repartition(partition).write()
                    .format("jdbc")
                    .mode(SaveMode.Overwrite)
                    .option("overwriteSchema", "true")
                    .option("url", instance.springDatasourceUrl)
                    .option("dbtable", sourceName)// "dbo.Employees2"
                    .option("user", instance.springDatasourceUsername)
                    .option("password", instance.springDatasourcePassword)
                    .save();

            dataImport.unpersist(true);

        }

        /* remove importing state */
        SparkService.dataStatus.remove(sourceName);

        /* update file import log */

        try {

            var entity = instance.tableImportedDao.findByTableName(sourceName);
            if (entity == null) {
                entity = new TableImported(sourceName);
            }

            var count = instance.tableCount(sourceName);

            entity.setRowsCount(BigDecimal.valueOf(count).add(entity.getRowsCount()));

            instance.tableImportedDao.save(entity);
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, "Update file import log Error", ex);
        } finally {

            /* delete temp file */

            Paths.get(path).toFile().delete();
        }

    }

    private synchronized static void processJoinSources(SparkService instance, List<String> sources) throws Exception {

        SparkService.dataStatus.put(SparkService.JOIN_TABLE_NAME,
                new String[] { SparkService.JOIN_TABLE_NAME, "-", "PREPARING", });

        /* create empty join table */

        var columns = new ArrayList<String>(Arrays.asList(AppColumnService.SOURCE_COLUMN));
        columns.addAll(instance.appColumnService.columnsToUpperCase());

        /* delete old join table */

        // instance.truncateTable(SparkService.JOIN_TABLE_NAME);
        instance.dropTableIfExists(SparkService.JOIN_TABLE_NAME);

        /* reset join info */

        var appInfo = instance.appInfoDao.findByAppInfo(SparkService.JOIN_TABLE_NAME);

        if (appInfo == null) {
            appInfo = new AppInfo();

            appInfo.setAppInfo(SparkService.JOIN_TABLE_NAME);
        }

        appInfo.setValue1("-");
        appInfo.setValue2("-");

        instance.appInfoDao.save(appInfo);

        /* create new join table with current column */

        instance.createTableIfNotExist(SparkService.JOIN_TABLE_NAME, columns);

        var joinColumns = instance.appColumnService.joins();

        /* prepare join query */

        var query = instance.generateSQLSelect(sources.toArray(String[]::new), columns.toArray(String[]::new),
                joinColumns.toArray(String[]::new), -1,
                null);

        query = "INSERT INTO " + SparkService.JOIN_TABLE_NAME + " " + query;

        /* run joining query */

        SparkService.dataStatus.put(SparkService.JOIN_TABLE_NAME,
                new String[] { SparkService.JOIN_TABLE_NAME, "-", "JOINING " + sources.toString(), });

        instance.jdbc.createStatement().execute(query);

        /* remove operation status */
        SparkService.dataStatus.remove(SparkService.JOIN_TABLE_NAME);

        /* update join info */

        var count = instance.tableCount(SparkService.JOIN_TABLE_NAME);

        appInfo.setValue1(String.valueOf(count));
        appInfo.setValue2(sources.toString());

        instance.appInfoDao.save(appInfo);

    }

    private Dataset<Row> joinBySpark(String[] tablesNames, String[] columns,
            String[] joinColumns) throws Exception {

        Dataset<Row> table1 = null;
        Dataset<Row> table2 = null;

        if (tablesNames.length > 0) {

            for (int i = 0; i < tablesNames.length; i++) {

                if (i == 0) {

                    table1 = this.sparkSession.read().format("jdbc").option("url", this.springDatasourceUrl)
                            // .option("query", query)
                            .option("dbtable", tablesNames[i])
                            .option("user", this.springDatasourceUsername)
                            .option("password", this.springDatasourcePassword)
                            .load().selectExpr(columns);

                } else {

                    table2 = table1;

                    table1 = this.sparkSession.read().format("jdbc").option("url", this.springDatasourceUrl)
                            // .option("query", query)
                            .option("dbtable", tablesNames[i])
                            .option("user", this.springDatasourceUsername)
                            .option("password", this.springDatasourcePassword)
                            .load().selectExpr(columns);

                    /* apply join */

                    /* prepare join expr */

                    Column colJoinExprs = null;
                    for (int j = 0; j < joinColumns.length; j++) {

                        var colName = joinColumns[j];

                        var colExpr = table2.col(colName).equalTo(table1.col(colName));

                        if (j == 0) {
                            colJoinExprs = colExpr;
                        } else {
                            // use OR SQL expression
                            colJoinExprs = colJoinExprs.or(colExpr);
                        }
                    }

                    if (colJoinExprs != null) {
                        table1 = table1.join(table2, colJoinExprs, "full");
                    } else {
                        table1 = table1.join(table2);
                    }

                    /* merge columns to avoid ambigues column */

                    if (this.appSparkJoinPartitions != null & !this.appSparkJoinPartitions.isEmpty()) {
                        table1 = table1.repartition(Integer.valueOf(this.appSparkJoinPartitions));
                    }

                    table1 = table1.map(new SparkService.JoinFunction(), table2.encoder()).selectExpr(columns);

                }

            }

            return table1;

        } else {
            throw new Exception("No table available");
        }
    }

    public void prepareJoinSources(List<String> sources) throws Exception {

        try {

            /* add informations according [TABLES_STATUS_HEADER] */

            SparkService.dataStatus.put(JOIN_TABLE_NAME,
                    new String[] { JOIN_TABLE_NAME, sources.toString(), "QUEUE", });

            SparkService.processJoinSources(this, sources);

        } catch (Exception ex) {

            /* set error status */

            SparkService.dataStatus.put(JOIN_TABLE_NAME,
                    new String[] { JOIN_TABLE_NAME, ex.getMessage(), "JOIN ERROR", });

            throw ex;
        }

    }

    public void prepareImportFile(String path, String sourceName, Map<String, String> columnsMapping,
            String delimiter,
            boolean excludeHeader) throws Exception {

        try {

            /* preparing file to import */

            /* Get file ou directory name to use as table [source] name */

            sourceName = sourceName.replaceAll("[^a-zA-Z0-9]", "");// remove all special char

            /* add informations according [TABLES_STATUS_HEADER] */

            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, "-", "QUEUE", });

            SparkService.processImportFile(this, path, sourceName, columnsMapping, delimiter, excludeHeader);

        } catch (Exception ex) {

            /* set error status */

            SparkService.dataStatus.put(sourceName,
                    new String[] { sourceName, ex.getMessage(), "IMPORT ERROR", });

            throw ex;
        }
    }

    public void truncateTable(String tableName) throws Exception {
        if (tableName != null) {
            this.jdbc.createStatement().execute("TRUNCATE TABLE " + tableName);

        } else {
            throw new Exception("table name is null");
        }
    }

    public void dropTableIfExists(String tableName) throws Exception {

        if (tableName != null) {
            this.jdbc.createStatement().execute("DROP TABLE IF EXISTS " + tableName);

            var entity = this.tableImportedDao.findByTableName(tableName);

            if (entity != null) {
                this.tableImportedDao.delete(entity);
            }

        } else {
            /* clear all tables if [tableName] in parameters is null */

            for (var e : this.allSources()) {
                this.dropTableIfExists(e);
            }

            this.tableImportedDao.deleteAll();
        }

    }

    public void createTableIfNotExist(String tableName, List<String> columns) throws Exception {

        if (tableName != null && columns != null && !columns.isEmpty()) {

            var query = "CREATE TABLE IF NOT EXISTS " + tableName;

            for (int i = 0; i < columns.size(); i++) {
                if (i == 0) {
                    query += " ( ";
                }

                query += columns.get(i) + " TEXT ";

                if (i != columns.size() - 1) {
                    query += " , ";
                } else {
                    query += " ) ";
                }
            }

            this.jdbc.createStatement().execute(query);

        } else {
            throw new Exception("table name is null or columns size is null");
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
    private synchronized Dataset<Row> queryBySQL(String[] tablesNames, String[] columns,
            String[] joinColumns, int limit, String filterExpression)
            throws Exception {

        if (tablesNames.length > 0) {

            var query = this.generateSQLSelect(tablesNames, columns, joinColumns, limit, filterExpression);

            LOGGER.info(query);

            var data = this.sparkSession.read().format("jdbc").option("url", this.springDatasourceUrl)
                    .option("query", query)
                    // .option("dbtable", "("+query+") foo")
                    .option("user", this.springDatasourceUsername).option("password", this.springDatasourcePassword)
                    .load();

            // var data = this.sparkSession.sqlContext().sql(query);
            data = data.selectExpr(columns);

            return data;

        } else {
            throw new Exception("No table available");
        }

    }

    private String generateSQLSelect(String[] tablesNames, String[] columns, String[] joinColumns, int limit,
            String filterExpression) {
        /* SELECT composition with columns (concatenation) */

        var query = " SELECT ";
        for (int i = 0; i < columns.length; i++) {
            var col = "";

            if (tablesNames.length == 1) {
                /* get unique column */
                col = columns[i];
            } else {
                /* Concatenate columns from many tables with system separator */
                col = " CONCAT_WS(\"" + SparkService.ITEM_SEPARATOR + "\", ";
                for (int j = 0; j < tablesNames.length; j++) {
                    col += "t" + j + "." + columns[i];

                    if (j != tablesNames.length - 1) {
                        col += " , ";
                    }
                }
                col += " ) AS " + columns[i];
            }

            /* add column to select */

            query += col;

            if (i != columns.length - 1) {
                query += " , ";
            }
        }

        /*
         * FROM : Apply join if many tables
         */

        if (tablesNames.length == 1) {

            /* Single table */

            query += " FROM " + tablesNames[0];

            if (filterExpression != null && !filterExpression.isEmpty()) {
                query += " WHERE " + filterExpression;
            }

            if (limit > 0) {
                query += " LIMIT " + limit;
            }
        } else {

            /*
             * JOIN composition : Simulate full join (left, right and union) if join columns
             * exist else apply natural join
             */

            if (joinColumns.length == 0) {

                /* Apply Natural Join */

                query += " FROM ";

                for (int i = 0; i < tablesNames.length; i++) {
                    if (i == 0) {
                        /* first tables just table without join */
                        query += tablesNames[i];
                    } else {
                        /* from second table then apply join */

                        query += " NATURAL JOIN " + tablesNames[i];

                    }
                }

                if (filterExpression != null && !filterExpression.isEmpty()) {
                    query += " WHERE " + filterExpression;
                }

                if (limit > 0) {
                    query += " LIMIT " + limit;
                }

            } else {

                /* Simulate Full Join : left, right and union */

                /* First : Left Join */

                var leftJoin = query + " FROM ";

                for (int i = 0; i < tablesNames.length; i++) {
                    if (i == 0) {
                        /* first tables just table without join */
                        leftJoin += tablesNames[i] + " t" + i;
                    } else {
                        /* from second table then apply join */

                        /* apply full just with preview table on specified columns */
                        leftJoin += " LEFT JOIN " + tablesNames[i] + " t" + i;
                        ;

                        for (int j = 0; j < joinColumns.length; j++) {
                            if (j == 0) {
                                leftJoin += " ON ";
                            } else {
                                leftJoin += " OR ";
                            }

                            /* i-1 to indicate preview table */
                            leftJoin += "t" + (i - 1) + "." + joinColumns[j] + " = t" + i + "."
                                    + joinColumns[j];
                        }

                    }
                }

                if (filterExpression != null && !filterExpression.isEmpty()) {
                    leftJoin += " WHERE " + filterExpression;
                }

                if (limit > 0) {
                    leftJoin += " LIMIT " + limit;
                }

                /* Second : Right Join */

                var rightJoin = query + " FROM ";

                for (int i = 0; i < tablesNames.length; i++) {
                    if (i == 0) {
                        /* first tables just table without join */
                        rightJoin += tablesNames[i] + " t" + i;
                        ;
                    } else {
                        /* from second table then apply join */

                        /* apply full just with preview table on specified columns */
                        rightJoin += " RIGHT JOIN " + tablesNames[i] + " t" + i;
                        ;

                        for (int j = 0; j < joinColumns.length; j++) {
                            if (j == 0) {
                                rightJoin += " ON ";
                            } else {
                                rightJoin += " OR ";
                            }

                            /* i-1 to indicate preview table */
                            rightJoin += "t" + (i - 1) + "." + joinColumns[j] + " = t" + i + "."
                                    + joinColumns[j];
                        }

                    }
                }

                if (filterExpression != null && !filterExpression.isEmpty()) {
                    rightJoin += " WHERE " + filterExpression;
                }

                if (limit > 0) {
                    rightJoin += " LIMIT " + limit;
                }

                /* Then apply union to simulate full join */

                query = "(" + leftJoin + ") UNION (" + rightJoin + ")";

            }
        }

        return query;
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
                .csv(filePath);

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

        List<String> tables = new ArrayList<String>(Arrays.asList(SparkService.JOIN_TABLE_NAME));
        tables.addAll(Arrays.asList(this.allSources()));

        for (var tableName : tables) {

            /* if table containt old column then rename */

            if (this.columnsByTable(tableName).contains(oldColumnName)) {

                this.jdbc.createStatement().execute(
                        "ALTER TABLE " + tableName + " RENAME COLUMN " + oldColumnName + " TO " + newColumnName);

            } else {
                /* else add new column to table with empty value */

                this.jdbc.createStatement().execute(
                        "ALTER TABLE " + tableName + " ADD COLUMN " + newColumnName + " TEXT");
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
        List<String> tables = new ArrayList<String>(Arrays.asList(SparkService.JOIN_TABLE_NAME));
        tables.addAll(Arrays.asList(this.allSources()));

        for (var tableName : tables) {

            /* if table containt column then remove */

            if (this.columnsByTable(tableName).contains(columnName)) {
                this.jdbc.createStatement().execute(
                        "ALTER TABLE " + tableName + " DROP COLUMN " + columnName);

            }
        }

        this.appColumnService.deleteColumnByName(columnName);

    }

    public List<String[]> tableStates() {
        var result = new ArrayList<String[]>();

        /* import states */

        for (var k : SparkService.dataStatus.keySet()) {
            result.add(SparkService.dataStatus.get(k));
        }

        // /* existants tables states */

        // List<String> tables = new
        // ArrayList<String>(Arrays.asList(SparkService.JOIN_TABLE_NAME));
        // tables.addAll(Arrays.asList(this.allSources()));

        // for (var e : tables) {

        // var count = this.tableCount(e);

        // result.add(new String[] { e,
        // String.valueOf(count), "DONE", });

        // }

        /* tables imported states */

        var tablesImported = this.tableImportedDao.findAll();

        for (var e : tablesImported) {

            var count = e.getRowsCount().longValue();

            result.add(new String[] { e.getTableName(),
                    String.valueOf(count), "DONE", });

        }

        /* join table state */

        var appInfo = this.appInfoDao.findByAppInfo(SparkService.JOIN_TABLE_NAME);

        if (appInfo != null) {
            result.add(new String[] { SparkService.JOIN_TABLE_NAME,
                    appInfo.getValue1(), appInfo.getValue2(), });
        } else {
            result.add(new String[] { SparkService.JOIN_TABLE_NAME,
                    "-", "EMPTY", });
        }

        return result;
    }

    public long tableCount(String tableName) {
        long result = 0L;

        try {
            var rs = this.jdbc.createStatement().executeQuery("SELECT count(*) FROM " + tableName);

            if (rs.next()) {
                result = rs.getInt(1);
            }

            return result;
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, ex.getMessage(), ex);
        }

        return result;

    }

    public File[] filesToImport() {

        var tempFolder = Paths.get(this.appWorkDir + File.separatorChar + "temp" + File.separatorChar + "import");

        if (!tempFolder.toFile().exists()) {
            tempFolder.toFile().mkdirs();
        }

        /* add files to exclude */

        var exclused = new ArrayList<File>();

        // exclude hide and system files and directory

        exclused.addAll(
                Arrays.asList(
                        tempFolder.toFile().listFiles(
                                new FileFilter() {

                                    public boolean accept(File file) {

                                        return file.isHidden() || file.isDirectory();
                                    }
                                })));

        /* apply exclude filter on working folder */

        var result =

                tempFolder.toFile().listFiles(new FileFilter() {

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

    public void updateJoin(String columnName, Boolean value) throws Exception {
        this.appColumnService.updateJoin(columnName, value);

    }

    public List<TableImported> tablesImported() {
        return this.tableImportedDao.findAll();
    }

    public List<String> columnsByTable(String tableName) throws SQLException {

        var result = new ArrayList<String>();

        try {
            var stm = this.jdbc.createStatement();
            stm.setMaxRows(1);
            var rs = stm.executeQuery("SELECT * FROM " + tableName);

            var rsMeta = rs.getMetaData();

            for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                result.add(rsMeta.getColumnName(i));
            }
        } catch (Exception ex) {
            LOGGER.log(Level.WARNING, ex.getMessage(), ex);
        }

        return result;

    }

    public String uploadFile(InputStream inputStream, String name) throws IOException {

        /* write input stream in local system as temp file */

        var tempFolder = Paths.get(this.appWorkDir + File.separatorChar + "temp" + File.separatorChar + "import");

        if (!tempFolder.toFile().exists()) {
            tempFolder.toFile().mkdirs();
        }

        var tempFile = new File(tempFolder.toString(), name);

        var outputStream = new FileOutputStream(tempFile);

        byte[] tampon = new byte[10240000];
        int eof = 0;

        while ((eof = inputStream.read(tampon, 0, tampon.length)) >= 0) {
            outputStream.write(tampon, 0, eof);
        }

        outputStream.close();
        inputStream.close();

        // /* check and/or create import dir in hadoop */

        // var importDirPath = new org.apache.hadoop.fs.Path(this.appWorkDir +
        // "/import");

        // var exist = this.hadoopFileSystem.exists(importDirPath);

        // if (!exist) {
        // this.hadoopFileSystem.mkdirs(importDirPath);
        // }

        // /* move temp file to hadoop */

        // var from = new org.apache.hadoop.fs.Path(tempFile.toString());
        // var to = new org.apache.hadoop.fs.Path(importDirPath.toString() + "/" +
        // tempFile.toFile().getName());

        // this.hadoopFileSystem.moveFromLocalFile(from, to);

        // /* delete temp file from local system */
        // tempFile.toFile().delete();

        // /* return path of destination file in hadoop */

        // return to.toString();

        return tempFile.getAbsolutePath();
    }

    @EventListener(ApplicationReadyEvent.class)
    public void createTableJoinIfNotExist() {

        try {

            this.createTableIfNotExist(SparkService.JOIN_TABLE_NAME, Arrays.asList(AppColumnService.SOURCE_COLUMN));

        } catch (Exception ex) {

            LOGGER.log(Level.SEVERE, "Create table join failed", ex);

        }
    }

    public List<String> allTablesInDB() {

        var resultat = new ArrayList<String>();

        try {

            var md = this.jdbc.getMetaData();
            var rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                // System.out.println(rs.getString(3));
                resultat.add(rs.getString(3));
            }

        } catch (Exception ex) {

            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

        }

        return resultat;
    }
}
