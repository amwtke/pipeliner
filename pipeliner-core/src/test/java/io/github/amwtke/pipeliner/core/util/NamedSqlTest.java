package io.github.amwtke.pipeliner.core.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class NamedSqlTest {
    @Test void simpleAndRepeated() {
        Map<String, Object> p = new HashMap<>();
        p.put("a", 1);
        p.put("b", "x");
        NamedSql q = NamedSql.parse("select * from t where a = :a and b = :b and c = :a", p);
        assertEquals("select * from t where a = ? and b = ? and c = ?", q.getSql());
        assertEquals(Arrays.<Object>asList(1, "x", 1), q.getArgs());
    }

    @Test void collectionsExpand() {
        Map<String, Object> p = new HashMap<>();
        p.put("ids", Arrays.asList(1, 2, 3));
        p.put("none", java.util.Collections.emptyList());
        NamedSql q = NamedSql.parse("x in (:ids) or y in (:none)", p);
        assertEquals("x in (?, ?, ?) or y in (NULL)", q.getSql());
        assertEquals(Arrays.<Object>asList(1, 2, 3), q.getArgs());
    }

    @Test void quotesAndCasts() {
        Map<String, Object> p = new HashMap<>();
        p.put("v", 7);
        NamedSql q = NamedSql.parse("select ':notparam', a::int, 'it''s :x' from t where k = :v", p);
        assertEquals("select ':notparam', a::int, 'it''s :x' from t where k = ?", q.getSql());
        assertEquals(Arrays.<Object>asList(7), q.getArgs());
    }

    @Test void nullValueAndTimeLiteral() {
        Map<String, Object> p = new HashMap<>();
        p.put("n", null);
        NamedSql q = NamedSql.parse("where t = '10:30' and n = :n and z = 1:2", p);
        assertEquals("where t = '10:30' and n = ? and z = 1:2", q.getSql());
        assertEquals(1, q.getArgs().size());
    }

    @Test void missingParam() {
        assertThrows(IllegalArgumentException.class, () -> NamedSql.parse("select :nope", new HashMap<String, Object>()));
    }

    /** Stand-in for driver-specific numeric wrappers such as ClickHouse UnsignedByte. */
    private static final class Unsigned extends Number {
        private final long v;
        Unsigned(long v) { this.v = v; }
        @Override public int intValue() { return (int) v; }
        @Override public long longValue() { return v; }
        @Override public float floatValue() { return v; }
        @Override public double doubleValue() { return v; }
        @Override public String toString() { return Long.toString(v); }
    }

    @Test void convertsAnyNumber() {
        assertEquals(Long.valueOf(7), NamedSql.convert(new Unsigned(7)));
        assertEquals(Long.valueOf(7), NamedSql.convert(Integer.valueOf(7)));
        assertEquals(new java.math.BigDecimal("1.5"), NamedSql.convert(Double.valueOf(1.5)));
        assertEquals(new java.math.BigDecimal("18446744073709551615"), NamedSql.convert(new java.math.BigInteger("18446744073709551615")));
    }

    @Test void commentsAndEscapesAreSkipped() {
        Map<String, Object> p = new HashMap<>();
        p.put("day", 3);
        NamedSql q = NamedSql.parse("-- don't touch :nope\nSELECT 1 WHERE d = :day", p);
        assertEquals("-- don't touch :nope\nSELECT 1 WHERE d = ?", q.getSql());
        assertEquals(Arrays.<Object>asList(3), q.getArgs());
        assertEquals("SELECT /* filter by :region */ ?", NamedSql.parse("SELECT /* filter by :region */ :day", p).getSql());
        assertEquals("SELECT ? # it's :x\n, ?", NamedSql.parse("SELECT :day # it's :x\n, :day", p).getSql());
        assertEquals("SELECT 'it\\'s :x', ?", NamedSql.parse("SELECT 'it\\'s :x', :day", p).getSql());
        assertEquals("SELECT 5-- 1\n, ?", NamedSql.parse("SELECT 5-- 1\n, :day", p).getSql());
        assertEquals("SELECT 5 /* unterminated :x", NamedSql.parse("SELECT 5 /* unterminated :x", p).getSql());
    }

    @Test void convertDriverTypes() {
        assertEquals(Arrays.asList(1L, 2L), NamedSql.convert(new Object[] {1, 2}));
        assertEquals(Arrays.asList(1L, 2L), NamedSql.convert(new long[] {1, 2}));
        assertEquals(Arrays.asList("a", null), NamedSql.convert(new String[] {"a", null}));
        assertEquals("2024-01-02T10:00:00", NamedSql.convert(java.time.LocalDateTime.of(2024, 1, 2, 10, 0)));
        assertEquals("2024-01-02T10:00:00", NamedSql.convert(java.sql.Timestamp.valueOf(java.time.LocalDateTime.of(2024, 1, 2, 10, 0))));
        assertEquals("2024-01-02", NamedSql.convert(java.time.LocalDate.of(2024, 1, 2)));
        assertEquals("2024-01-02T08:00:00", NamedSql.convert(java.time.OffsetDateTime.of(2024, 1, 2, 10, 0, 0, 0, java.time.ZoneOffset.ofHours(2))));
        assertEquals(null, NamedSql.convert(Double.NaN));
        assertEquals(null, NamedSql.convert(Float.POSITIVE_INFINITY));
        assertThrows(io.github.amwtke.pipeliner.api.PipelinerException.class, () -> NamedSql.convert(new Object()));
    }
}
