package io.github.amwtke.pipeliner.api;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Canonical row value types. A value stored in a {@link Row} is normalized to exactly the type the JSON chunk codec
 * returns after a round trip, so a runner sees the same types in INLINE, LOCAL and CLUSTER mode:
 * <ul>
 * <li>null, String, Boolean, Long (Integer/Short/Byte widen to Long);</li>
 * <li>BigDecimal with a positive scale (Float/Double/BigDecimal); integral decimals and BigInteger become Long, or
 *     a scale-1 BigDecimal when beyond the long range;</li>
 * <li>Float/Double NaN and +-Infinity have no JSON/decimal form and are stored as null;</li>
 * <li>java.time values and java.util.Date become a canonical ISO-8601 String (local date-time always with seconds;
 *     OffsetDateTime/ZonedDateTime/Instant are rendered in UTC with a trailing 'Z');</li>
 * <li>List/array of canonical values (Lists are copied) and Map with String keys of canonical values;</li>
 * <li>anything else is rejected with an IllegalArgumentException.</li>
 * </ul>
 */
public final class RowValues {
    private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE), LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

    private RowValues() {}

    /** Canonical local date-time text: ISO_LOCAL_DATE_TIME with seconds always present. */
    public static String formatLocalDateTime(LocalDateTime t) {
        return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(t);
    }

    public static Object normalize(Object v) {
        if (v == null || v instanceof String || v instanceof Boolean || v instanceof Long) return v;
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) return ((Number) v).longValue();
        if (v instanceof Character) return v.toString();
        if (v instanceof Float || v instanceof Double) {
            double d = ((Number) v).doubleValue();
            if (Double.isNaN(d) || Double.isInfinite(d)) return null;
            return decimal(new BigDecimal(v.toString()));
        }
        if (v instanceof BigDecimal) return decimal((BigDecimal) v);
        if (v instanceof BigInteger) return integer((BigInteger) v);
        if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate().toString();
        if (v instanceof Timestamp) return formatLocalDateTime(((Timestamp) v).toLocalDateTime());
        if (v instanceof Time) return v.toString();
        if (v instanceof java.util.Date) return DateTimeFormatter.ISO_INSTANT.format(((java.util.Date) v).toInstant());
        if (v instanceof LocalDateTime) return formatLocalDateTime((LocalDateTime) v);
        if (v instanceof LocalDate) return v.toString();
        if (v instanceof LocalTime) return DateTimeFormatter.ISO_LOCAL_TIME.format((LocalTime) v);
        if (v instanceof OffsetDateTime) return utc(((OffsetDateTime) v).toInstant());
        if (v instanceof ZonedDateTime) return utc(((ZonedDateTime) v).toInstant());
        if (v instanceof Instant) return utc((Instant) v);
        if (v instanceof Collection) {
            List<Object> l = new ArrayList<>(((Collection<?>) v).size());
            for (Object o : (Collection<?>) v) l.add(normalize(o));
            return l;
        }
        if (v instanceof Object[]) {
            Object[] a = (Object[]) v;
            List<Object> l = new ArrayList<>(a.length);
            for (Object o : a) l.add(normalize(o));
            return l;
        }
        if (v instanceof Map) {
            Map<String, Object> m = new LinkedHashMap<>();
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                if (!(e.getKey() instanceof String))
                    throw new IllegalArgumentException("unsupported row map key type " + (e.getKey() == null ? "null" : e.getKey().getClass().getName()) + " (keys must be String)");
                m.put((String) e.getKey(), normalize(e.getValue()));
            }
            return m;
        }
        throw new IllegalArgumentException("unsupported row value type " + v.getClass().getName()
                + "; use null, String, Boolean, Long/Integer, BigDecimal/Double, java.time, or a List/Map of those");
    }

    private static String utc(Instant i) {
        return formatLocalDateTime(LocalDateTime.ofInstant(i, ZoneOffset.UTC)) + "Z";
    }

    private static Object decimal(BigDecimal d) {
        return d.scale() > 0 ? d : integer(d.toBigInteger());   // positive scale survives the plain-JSON round trip as BigDecimal
    }

    private static Object integer(BigInteger b) {
        return b.compareTo(LONG_MIN) >= 0 && b.compareTo(LONG_MAX) <= 0 ? (Object) b.longValue() : new BigDecimal(b).setScale(1);   // beyond long: a scale-1 BigDecimal ("...0.0") round-trips, a huge JSON integer does not
    }
}
