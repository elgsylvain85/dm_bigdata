package com.dm.bigdata.model.pojo;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import lombok.Data;

@Entity
@Data
public class AppColumn {

    @Id
    String id;
    @Column(unique = true, nullable = false)
    String columnName;
    // SchemaType type;
    // Boolean nullable;
    // @Column(nullable = false)
    Boolean joinColum;

    // @OneToOne(cascade = CascadeType.ALL)
    // JoinSetting joinSetting;

    public AppColumn() {
        this.id = UUID.randomUUID().toString();
        // this.nullable = true;
        // this.type = SchemaType.STRING;
        this.joinColum = false;
    }

    public AppColumn(String name) {

        this();
        this.columnName = name.replaceAll("[^a-zA-Z0-9]", "");// remove all special char;

    }

    public static List<AppColumn> defaultData() {

        var result = new ArrayList<AppColumn>();

        for (int i = 0; i < 6; i++) {

            result.add(new AppColumn("Column" + (i + 1)));

        }
        return result;
    }

    // public static enum SchemaType {
    //     STRING, INTEGER, DOUBLE;
    // }

    /////////////// SETTERS///////////////////
    public void setColumnName(String columnName) {
        columnName = columnName.replaceAll("[^a-zA-Z0-9]", "");// remove all special char
        columnName = columnName.toUpperCase();
        this.columnName = columnName;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        else {
            var o = (AppColumn) other;
            return this.id.equals(o.id);
        }
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }
}
