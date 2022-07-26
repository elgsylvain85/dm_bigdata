package com.dm.bigdata.model.pojo;

import java.math.BigDecimal;
import java.util.UUID;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;

import lombok.Data;

@Entity
@Data
public class TableImported {

    @Id
    String id;
    @Column(unique = true, nullable = false)
    String tableName;
    BigDecimal rowsCount;

    public TableImported() {
        this.id = UUID.randomUUID().toString();
        this.rowsCount = new BigDecimal(0);
    }

    public TableImported(String tableName) {
        this();
        this.tableName = tableName;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null)
            return false;
        else {
            var o = (TableImported) other;
            return this.id.equals(o.id);
        }
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

}
