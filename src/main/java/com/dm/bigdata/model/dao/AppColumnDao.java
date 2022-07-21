package com.dm.bigdata.model.dao;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.dm.bigdata.model.pojo.AppColumn;

public interface AppColumnDao extends JpaRepository<AppColumn, String> {

    List<AppColumn> findByJoinColum(Boolean joinColum);

    AppColumn findByColumnName(String columnName);

}
