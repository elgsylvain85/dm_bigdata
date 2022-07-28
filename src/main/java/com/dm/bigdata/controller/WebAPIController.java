package com.dm.bigdata.controller;

import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.function.Function;
import java.util.logging.Level;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.dm.bigdata.model.pojo.TableImported;
import com.dm.bigdata.model.service.AppColumnService;
import com.dm.bigdata.model.service.SparkService;
import com.fasterxml.jackson.databind.ObjectMapper;

@RestController
@RequestMapping("/webapi")
@CrossOrigin("*")
public class WebAPIController {

    static final Logger LOGGER = Logger.getLogger(WebAPIController.class.getName());

    @Autowired
    AppColumnService appColumnService;

    @Autowired
    SparkService sparkService;

    @Autowired
    ObjectMapper jsonMapper;

    @GetMapping(value = "/filestructure")
    public ResponseEntity<?> fileStructure(@RequestParam String filePath,
            @RequestParam(required = false, defaultValue = ",") String delimiter,
            @RequestParam(required = false, defaultValue = "false") Boolean excludeHeader, @RequestParam(required = false, defaultValue = "10") Integer limit) {

        try {

            var data = this.sparkService.fileStructure(filePath, delimiter, excludeHeader, limit);

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/filestoimport")
    public ResponseEntity<?> filesToImport() {

        try {
            var data = this.sparkService.filesToImportAsPath();
            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/appcolumns")
    public ResponseEntity<?> appColumns() {

        try {
            var data = this.appColumnService.columnsToUpperCase();

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/alljoins")
    public ResponseEntity<?> allJoins() {

        try {
            var data = this.appColumnService.joins();

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/appcolumnswithsource")
    public ResponseEntity<?> appcolumnsWithSource() {

        try {
            var data = this.appColumnService.columnsWithSource();

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/tablesimported")
    public ResponseEntity<?> tablesImported() {

        try {

            var data = this.sparkService.tablesImported().stream().map(
                    new Function<TableImported, String>() {

                        @Override
                        public String apply(TableImported t) {
                            return t.getTableName();
                        }

                    }).collect(Collectors.toList());

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @PostMapping(value = "/importfile")
    public ResponseEntity<?> importFile(@RequestParam String filePath, @RequestParam(required = false) String tableName,
            @RequestParam String fileStructure, @RequestParam(required = false, defaultValue = ",") String delimiter,
            @RequestParam(required = false, defaultValue = "false") Boolean excludeHeader) {
        try {

            Map<String, String> structure = this.jsonMapper.readValue(fileStructure, Map.class);

            /* run import as independent job (thread) */

            new Thread(() -> {
                try {
                    this.sparkService.prepareImportFile(filePath, tableName, structure, delimiter, excludeHeader);
                } catch (Exception ex) {
                    LOGGER.log(Level.SEVERE, "Job import Error", ex);
                }
            }).start();

            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/data", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> fileData(@RequestParam(required = false, defaultValue = "0") Integer offset,
            @RequestParam(required = false, defaultValue = "100") Integer limit,
            // @RequestParam(required = false) String tableName,
            @RequestParam(required = false) String filter) {

        try {

            // var data = this.sparkService.data(tableName, offset, limit, filter);
            var data = this.sparkService.dataAsMap(offset, limit, filter);

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/exportresult")
    public ResponseEntity<?> exportResult(
            @RequestParam(required = false) String filter) {

        try {

            /* run export as independent job (thread) */

            new Thread(() -> {
                try {
                    this.sparkService.exportResult(filter);
                } catch (Exception ex) {
                    LOGGER.log(Level.SEVERE, "Job export Error", ex);
                }
            }).start();

            return ResponseEntity.ok().build();

        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @PostMapping(value = "/droptable")
    public ResponseEntity<?> removeFile(@RequestParam(required = false) String tableName) {
        try {
            this.sparkService.dropTable(tableName);
            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @PostMapping(value = "/updateappcolumn")
    public ResponseEntity<?> updateAppColumn(
            @RequestParam(required = false) String oldColumnName, @RequestParam String newColumnName) {
        try {

            this.sparkService.updateColumnByName(oldColumnName, newColumnName);

            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @PostMapping(value = "/deletecolumn")
    public ResponseEntity<?> deleteAppColumn(
            @RequestParam String columnName) {
        try {

            this.sparkService.deleteColumnByName(columnName);

            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @PostMapping(value = "/updatejoin")
    public ResponseEntity<?> updateJoin(
            @RequestParam String columnName, @RequestParam Boolean value) {
        try {

            this.sparkService.updateJoin(columnName, value);

            return ResponseEntity.ok().build();
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);
            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/tablesstatus", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> tablesStatus() {

        try {

            var data = this.sparkService.tablesStatus();

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            // ex.printStackTrace();
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }

    @GetMapping(value = "/tablesstatusheader", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> tablesStatusHeader() {

        try {

            var data = SparkService.TABLES_STATUS_HEADER;

            return ResponseEntity.ok(data);
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, ex.getMessage(), ex);

            return ResponseEntity.badRequest().body(ex.getMessage());
        }
    }
}
