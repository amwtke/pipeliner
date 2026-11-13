package io.github.amwtke.pipeliner.example;

import static org.junit.jupiter.api.Assertions.*;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.github.amwtke.pipeliner.core.util.SimpleDataSourceRegistry;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Seeds twice against real MySQL/ClickHouse and compares a checksum of the master data. */
class SeederTest {
    private static boolean reachable(int port) {
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress("127.0.0.1", port), 500);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static DataSource ds(String url, String driver) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(url); c.setUsername("pipeliner"); c.setPassword("pipeliner"); c.setDriverClassName(driver);
        c.setMaximumPoolSize(2);
        return new HikariDataSource(c);
    }

    @Test
    void seedingTwiceIsIdentical() throws Exception {
        Assumptions.assumeTrue(reachable(3306) && reachable(8123));
        DataSource my = ds("jdbc:mysql://127.0.0.1:3306/pipeliner_demo?createDatabaseIfNotExist=true&useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true&serverTimezone=UTC", "com.mysql.cj.jdbc.Driver");
        DataSource ch = ds("jdbc:clickhouse:http://127.0.0.1:8123/default?compress=0", "com.clickhouse.jdbc.ClickHouseDriver");
        Map<String, DataSource> m = new HashMap<>();
        m.put("mysql", my);
        m.put("ch", ch);
        Seeder seeder = new Seeder(new SimpleDataSourceRegistry(m));
        Map<String, Object> c1 = seeder.seed(60, 42);
        String h1 = fingerprint(my);
        Map<String, Object> c2 = seeder.seed(60, 42);
        assertEquals(c1, c2);
        assertEquals(h1, fingerprint(my));
        assertEquals(60, c1.get("ex_material"));
        assertEquals(60 * 5, c1.get("ex_inventory"));
        assertEquals(48, c1.get("ex_bom"));
        seeder.seed(60, 7);
        assertNotEquals(h1, fingerprint(my));
    }

    private static String fingerprint(DataSource ds) throws Exception {
        long h = 0;
        String[] qs = {"select concat_ws('|',material_id,group_id,kind,eol_month) from ex_material",
                "select concat_ws('|',material_id,node,qty) from ex_inventory",
                "select concat_ws('|',material_id,node,month,qty) from ex_in_transit",
                "select concat_ws('|',material_id,node,qty) from ex_safety_stock",
                "select concat_ws('|',big_id,fine_id,usage_qty,recovery_rate) from ex_bom",
                "select concat_ws('|',site,month,capacity) from ex_site_capacity"};
        try (Connection c = ds.getConnection()) {
            for (String q : qs) {
                try (PreparedStatement ps = c.prepareStatement(q); ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) h += Seeder.rowHash(q.length() + rs.getString(1));
                }
            }
        }
        return Long.toHexString(h);
    }
}
