package io.github.amwtke.pipeliner.api;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Ordered column-to-value record with lenient typed getters. */
public final class Row implements Serializable {
    private static final long serialVersionUID = 1L;
    private final LinkedHashMap<String, Object> values = new LinkedHashMap<>();

    public Row() {}

    public Row(Map<String, ?> values) {
        if (values != null) this.values.putAll(values);
    }

    /** Builds a row from alternating column/value arguments. */
    public static Row of(Object... kv) {
        if (kv.length % 2 != 0) throw new IllegalArgumentException("Row.of needs an even number of arguments");
        Row r = new Row();
        for (int i = 0; i < kv.length; i += 2) r.values.put(String.valueOf(kv[i]), kv[i + 1]);
        return r;
    }

    public Object get(String col) { return values.get(col); }

    public String getString(String col) {
        Object v = values.get(col);
        return v == null ? null : v.toString();
    }

    public Long getLong(String col) {
        Object v = values.get(col);
        if (v == null) return null;
        if (v instanceof Long) return (Long) v;
        if (v instanceof Number) {
            if (v instanceof BigDecimal) return ((BigDecimal) v).longValue();
            return ((Number) v).longValue();
        }
        if (v instanceof Boolean) return ((Boolean) v) ? 1L : 0L;
        String s = v.toString().trim();
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            return new BigDecimal(s).longValue();
        }
    }

    public Integer getInt(String col) {
        Long l = getLong(col);
        return l == null ? null : Integer.valueOf(l.intValue());
    }

    public BigDecimal getDecimal(String col) {
        Object v = values.get(col);
        if (v == null) return null;
        if (v instanceof BigDecimal) return (BigDecimal) v;
        if (v instanceof BigInteger) return new BigDecimal((BigInteger) v);
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte)
            return BigDecimal.valueOf(((Number) v).longValue());
        if (v instanceof Number) return BigDecimal.valueOf(((Number) v).doubleValue());
        return new BigDecimal(v.toString().trim());
    }

    public Double getDouble(String col) {
        Object v = values.get(col);
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).doubleValue();
        return Double.valueOf(v.toString().trim());
    }

    public Boolean getBool(String col) {
        Object v = values.get(col);
        if (v == null) return null;
        if (v instanceof Boolean) return (Boolean) v;
        if (v instanceof Number) return ((Number) v).doubleValue() != 0;
        String s = v.toString().trim();
        if ("1".equals(s)) return Boolean.TRUE;
        if ("0".equals(s)) return Boolean.FALSE;
        return Boolean.valueOf(s);
    }

    public Row set(String col, Object value) {
        values.put(col, value);
        return this;
    }

    public boolean has(String col) { return values.containsKey(col); }

    public Set<String> columns() { return Collections.unmodifiableSet(values.keySet()); }

    /** Unmodifiable view in insertion order. */
    public Map<String, Object> asMap() { return Collections.unmodifiableMap(values); }

    public Row copy() { return new Row(values); }

    @Override public boolean equals(Object o) { return o instanceof Row && values.equals(((Row) o).values); }

    @Override public int hashCode() { return values.hashCode(); }

    @Override public String toString() { return "Row" + values; }
}
