package com.dm.bigdata.model.pojo;

import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import lombok.Data;

@Entity
@Data
public class AppInfo {

    @Id
    String id;
    @Column(nullable = false, unique = true)
    String appInfo;
    String value1;
    String value2;

    public AppInfo() {
        this.id = UUID.randomUUID().toString();
    }

    // public static enum SettingType {
    // IMPORT_DELIMITER, IMPORT_HEADER_IN, WORK_FOLDER
    // }

    // public static List<AppSetting> defaultData() {
    // var delimiter = new AppSetting(SettingType.IMPORT_DELIMITER, ";");
    // var headerIn = new AppSetting(SettingType.IMPORT_HEADER_IN, "false");
    // var workFolder = new AppSetting(SettingType.WORK_FOLDER,
    // System.getProperty("java.io.tmpdir")+"dm-bigdata");

    // var result = new ArrayList<AppSetting>();

    // result.add(delimiter);
    // result.add(headerIn);
    // result.add(workFolder);

    // return result;
    // }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        else {
            var o = (AppInfo) other;
            return this.id.equals(o.id);
        }
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        return "AppInfo [appInfo=" + appInfo + ", id=" + id + ", value1=" + value1 + ", value2=" + value2 + "]";
    }
}
