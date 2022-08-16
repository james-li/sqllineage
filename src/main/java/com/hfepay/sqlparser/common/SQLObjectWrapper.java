package com.hfepay.sqlparser.common;

import com.alibaba.druid.sql.ast.SQLObject;
import com.alibaba.druid.sql.ast.SQLObjectImpl;

import java.io.Serializable;
import java.lang.reflect.Method;

//因为SQLSelectItem的相等不比较parent，所以在查找储存select item
//对应的expr时出错
public class SQLObjectWrapper<T > implements Serializable {
    final private T item;

    public SQLObjectWrapper(T item) {
        this.item = item;
    }

    public SQLObject getObject() {
        if(this.item instanceof SQLObject)
            return (SQLObject) this.item;
        else
            return null;
    }


    @Override
    public int hashCode() {
        return this.item.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        SQLObject newObject = null;
        SQLObject object = this.getObject();
        if (object == null)
            return false;
        if (obj.getClass() == this.item.getClass())
            newObject = (SQLObject) obj;
        else if (obj.getClass() == this.getClass()) {
            newObject = ((SQLObjectWrapper<?>) obj).getObject();
        } else
            return false;
        for (; object != null && newObject != null; object = object.getParent(), newObject = newObject.getParent()) {
            if (!object.equals(newObject))
                return false;
        }
        return object == null && newObject == null;
    }

    public String toString() {
        return item.toString();
    }

}
