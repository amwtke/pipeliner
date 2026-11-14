package io.github.amwtke.pipeliner.core.codec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.amwtke.pipeliner.api.Chunk;
import io.github.amwtke.pipeliner.api.Row;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Row values are canonical at set time, so every execution mode sees what a CLUSTER round trip yields. */
class RowValuesTest {
    private static Row roundTrip(Row r) {
        return ChunkCodec.decodeChunk(ChunkCodec.encodeChunk(new Chunk("c", new ArrayList<>(Collections.singletonList(r)), Collections.<String, String>emptyMap()))).getRows().get(0);
    }

    @Test void numbersAreCanonical() {
        Row r = Row.of("i", 7, "s", (short) 3, "b", (byte) 1, "d", 1.5d, "f", 0.1f, "bd", new BigDecimal("12.50"), "bd0", new BigDecimal("5"),
                "big", new BigInteger("18446744073709551615"), "dz", 5.0d);
        assertEquals(7L, r.get("i"));
        assertEquals(3L, r.get("s"));
        assertEquals(1L, r.get("b"));
        assertEquals(new BigDecimal("1.5"), r.get("d"));
        assertEquals(new BigDecimal("0.1"), r.get("f"));
        assertEquals(5L, r.get("bd0"));
        assertEquals(new BigDecimal("18446744073709551615.0"), r.get("big"));
        assertEquals(r, roundTrip(r));
        assertEquals(r.get("dz"), roundTrip(r).get("dz"));
    }

    @Test void nanAndInfinityBecomeNull() {
        Row r = Row.of("n", Double.NaN, "p", Double.POSITIVE_INFINITY, "q", Float.NEGATIVE_INFINITY).set("x", Double.NaN);
        assertNull(r.get("n"));
        assertNull(r.get("p"));
        assertNull(r.get("q"));
        assertNull(r.get("x"));
        assertTrue(r.has("n"));
        assertEquals(r, roundTrip(r));
    }

    @Test void javaTimeAndDatesBecomeIsoStrings() {
        Row r = Row.of("d", java.time.LocalDate.of(2024, 1, 1), "t", java.time.LocalDateTime.of(2024, 1, 2, 10, 0),
                "o", java.time.OffsetDateTime.of(2024, 1, 2, 10, 0, 0, 0, java.time.ZoneOffset.ofHours(2)),
                "ts", java.sql.Timestamp.valueOf("2024-01-02 10:00:00"), "u", new java.util.Date(0L));
        assertEquals("2024-01-01", r.get("d"));
        assertEquals("2024-01-02T10:00:00", r.get("t"));
        assertEquals("2024-01-02T08:00:00Z", r.get("o"));
        assertEquals("2024-01-02T10:00:00", r.get("ts"));
        assertEquals("1970-01-01T00:00:00Z", r.get("u"));
        assertEquals(r, roundTrip(r));   // used to throw in CLUSTER mode only (no JSR-310 module)
    }

    @Test void listsMapsAndRejectedTypes() {
        Row r = Row.of("l", Arrays.asList(1, 2.5d, "x"), "a", new Object[] {1, null}).set("m", Collections.singletonMap("k", 3));
        assertEquals(Arrays.<Object>asList(1L, new BigDecimal("2.5"), "x"), r.get("l"));
        assertEquals(r, roundTrip(r));
        assertThrows(IllegalArgumentException.class, () -> new Row().set("x", new Object()));
        assertThrows(IllegalArgumentException.class, () -> Row.of("x", new StringBuilder("a")));
        assertEquals(r, r.copy());
    }
}
