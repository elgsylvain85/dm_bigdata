package com.dm.bigdata.model.dao;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.dm.bigdata.model.pojo.AppColumn;
import com.dm.bigdata.model.pojo.TableImported;

public interface TableImportedDao extends JpaRepository<TableImported, String> {


    TableImported findByTableName(String tableName);

}
