package com.dm.bigdata.model.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.dm.bigdata.model.dao.AppColumnDao;
import com.dm.bigdata.model.pojo.AppColumn;

@Service
public class AppColumnService {

    static final String SOURCE_COLUMN = "SOURCES";

    @Autowired
    AppColumnDao appColumnDao;

    public List<AppColumn> allColumns() {
        return appColumnDao.findByOrderByColumnNameAsc();
    }

    public List<String> columnsToUpperCase() {

        var result = new ArrayList<String>();

        var columns = this.allColumns();

        for (var col : columns) {
            result.add(col.getColumnName().toUpperCase());
        }

        return result;

    }

    public List<String> columnsWithSource() throws Exception {

        var columns = new ArrayList<String>();
        /* source first */
        columns.add(AppColumnService.SOURCE_COLUMN);
        columns.addAll(this.columnsToUpperCase());

        return columns;
    }

    public void updateColumn(String id, String columnName) {

        AppColumn entity = null;

        if (id == null) {
            entity = new AppColumn();
        } else {
            var oEntity = this.appColumnDao.findById(id);
            if (oEntity.isPresent()) {
                entity = oEntity.get();
            } else {
                entity = new AppColumn();
            }
        }

        entity.setColumnName(columnName);

        this.appColumnDao.save(entity);

    }

    public void deleteColumn(String id) {
        this.appColumnDao.deleteById(id);

    }

    public long schemasCount() {
        return this.appColumnDao.count();
    }

    public void addJoin(String schemaId) {
        var oSchema = this.appColumnDao.findById(schemaId);

        if (oSchema.isPresent()) {

            oSchema.get().setJoinColum(true);
            this.appColumnDao.save(oSchema.get());

        } else {
            throw new RuntimeException("schema not exist : id -> " + schemaId);
        }
    }

    public void removeJoin(String schemaId) {
        var oSchema = this.appColumnDao.findById(schemaId);

        if (oSchema.isPresent()) {

            oSchema.get().setJoinColum(false);
            this.appColumnDao.save(oSchema.get());

        } else {
            throw new RuntimeException("schema not exist : id -> " + schemaId);
        }
    }

    public List<AppColumn> allJoins() {
        return this.appColumnDao.findByJoinColumOrderByColumnNameAsc(true);
    }

    public List<String> joins() {
        var result = new ArrayList<String>();

        var joins = this.allJoins();

        for (var col : joins) {
            result.add(col.getColumnName());
        }

        return result;
    }

    public void updateColumnByName(String oldColumnName, String newColumnName) {

        AppColumn entity = null;

        if (oldColumnName == null) {
            entity = new AppColumn();
        } else {
            entity = this.appColumnDao.findByColumnName(oldColumnName);

            if (entity == null) {
                entity = new AppColumn();
            }
        }

        entity.setColumnName(newColumnName);

        this.appColumnDao.save(entity);
    }

    public void deleteColumnByName(String columnName) {

        AppColumn entity = this.appColumnDao.findByColumnName(columnName);

        this.appColumnDao.deleteById(entity.getId());
    }

    public void updateJoin(String columnName, Boolean value) {

        AppColumn entity = this.appColumnDao.findByColumnName(columnName);

        entity.setJoinColum(value);

        this.appColumnDao.save(entity);
    }
}
