package com.hfepay.sqlparser.common;

import java.io.Serializable;

public class Tuple<V1, V2> implements Serializable {
    private static final long serialVersionUID = 6848965487886590984L;
    private V1 val1;
    private V2 val2;

    public Tuple(V1 val1, V2 val2) {
        this.val1 = val1;
        this.val2 = val2;
    }

    public V1 getVal1() {
        return val1;
    }

    public void setVal1(V1 val1) {
        this.val1 = val1;
    }

    public void setVal(V1 val1, V2 val2) {
        setVal1(val1);
        setVal2(val2);
    }
    public V2 getVal2() {
        return val2;
    }

    public void setVal2(V2 val2) {
        this.val2 = val2;
    }

    public String toString() {
        return val1 + " : " + val2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple<?, ?> tuple = (Tuple<?, ?>) o;

        if (!val1.equals(tuple.val1)) return false;
        return val2.equals(tuple.val2);
    }

    @Override
    public int hashCode() {
        int result = val1.hashCode();
        result = 31 * result + val2.hashCode();
        return result;
    }
}
