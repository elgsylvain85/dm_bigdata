package com.dm.bigdata.model.dao;

import org.springframework.data.jpa.repository.JpaRepository;

import com.dm.bigdata.model.pojo.AppInfo;

public interface AppInfoDao extends JpaRepository<AppInfo, String>{
    AppInfo findByAppInfo(String appInfo);
}
