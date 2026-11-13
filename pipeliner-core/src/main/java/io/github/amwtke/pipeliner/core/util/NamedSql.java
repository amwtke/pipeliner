package io.github.amwtke.pipeliner.core.util;

import io.github.amwtke.pipeliner.api.PipelinerException;
import io.github.amwtke.pipeliner.api.Row;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;

/** Named-parameter SQL ("... :name ...") converted to positional JDBC SQL. */
public final class NamedSql {
    private final String sql;
    private final List<Object> args;

    private NamedSql(String sql, List<Object> args) {
        this.sql = sql;
        this.args = Collections.unmodifiableList(args);
    }

    public String getSql() { return sql; }
    public List<Object> getArgs() { return args; }

    /** Parses ":name" placeholders; Collection values expand to "?, ?, ?" (empty collection becomes NULL). */
    public static NamedSql parse(String sql, Map<String, ?> params) {
        StringBuilder out = new StringBuilder(sql.length());
        List<Object> args = new ArrayList<>();
        int n = sql.length();
        char quote = 0;
        for (int i = 0; i < n; i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                out.append(c);
                if (c == quote) quote = 0;
                continue;
            }
            if (c == '\'' || c == '"' || c == '`') {
                quote = c;
                out.append(c);
            } else if (c == ':' && i + 1 < n && sql.charAt(i + 1) == ':') {
                out.append("::");
                i++;
            } else if (c == ':' && i + 1 < n && isStart(sql.charAt(i + 1))) {
                int j = i + 1;
                while (j < n && isPart(sql.charAt(j))) j++;
                String name = sql.substring(i + 1, j);
                if (params == null || !params.containsKey(name)) throw new IllegalArgumentException("missing SQL parameter: " + name);
                Object v = params.get(name);
                if (v instanceof Object[]) v = java.util.Arrays.asList((Object[]) v);
                if (v instanceof Collection) {
                    Collection<?> col = (Collection<?>) v;
                    if (col.isEmpty()) {
                        out.append("NULL");
                    } else {
                        boolean first = true;
                        for (Object o : col) {
                            out.append(first ? "?" : ", ?");
                            first = false;
                            args.add(o);
                        }
                    }
                } else {
                    out.append('?');
                    args.add(v);
                }
                i = j - 1;
            } else {
                out.append(c);
            }
        }
        return new NamedSql(out.toString(), args);
    }

    private static boolean isStart(char c) { return Character.isLetter(c) || c == '_'; }
    private static boolean isPart(char c) { return Character.isLetterOrDigit(c) || c == '_'; }

    /** Runs a query and maps each result row; column names are lower-cased labels. */
    public static List<Row> query(DataSource ds, String sql, Map<String, ?> params) {
        NamedSql q = parse(sql, params);
        try (Connection con = ds.getConnection(); PreparedStatement ps = con.prepareStatement(q.sql)) {
            for (int i = 0; i < q.args.size(); i++) ps.setObject(i + 1, q.args.get(i));
            try (ResultSet rs = ps.executeQuery()) {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                String[] names = new String[cols];
                for (int i = 0; i < cols; i++) names[i] = md.getColumnLabel(i + 1).toLowerCase(Locale.ROOT);
                List<Row> rows = new ArrayList<>();
                while (rs.next()) {
                    Row r = new Row();
                    for (int i = 0; i < cols; i++) r.set(names[i], convert(rs.getObject(i + 1)));
                    rows.add(r);
                }
                return rows;
            }
        } catch (SQLException e) {
            throw new PipelinerException("query failed: " + e.getMessage() + " [" + q.sql + "]", e);
        }
    }

    /** JDBC value to JSON-friendly value. */
    public static Object convert(Object v) {
        if (v == null) return null;
        if (v instanceof Long || v instanceof Boolean || v instanceof String) return v;
        if (v instanceof Integer || v instanceof Short || v instanceof Byte) return ((Number) v).longValue();
        if (v instanceof BigDecimal) return v;
        if (v instanceof BigInteger) return new BigDecimal((BigInteger) v);
        if (v instanceof Float || v instanceof Double) return new BigDecimal(v.toString());
        if (v instanceof Number) { // driver-specific numeric wrappers, e.g. ClickHouse UnsignedByte (UInt8)
            BigDecimal d = new BigDecimal(v.toString());
            return d.scale() <= 0 && d.precision() <= 18 ? (Object) d.longValue() : d;
        }
        if (v instanceof java.sql.Date) return ((java.sql.Date) v).toLocalDate().toString();
        if (v instanceof Timestamp) return DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(((Timestamp) v).toLocalDateTime());
        if (v instanceof Time) return v.toString();
        if (v instanceof byte[]) return Base64.getEncoder().encodeToString((byte[]) v);
        return v.toString();
    }
}
