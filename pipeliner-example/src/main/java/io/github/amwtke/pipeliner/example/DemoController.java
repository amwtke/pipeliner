package io.github.amwtke.pipeliner.example;

import io.github.amwtke.pipeliner.core.engine.PipelinerEngine;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Demo convenience endpoints: seed, submit, checksum. */
@RestController
@RequestMapping("/demo")
public class DemoController {
    private final Seeder seeder;
    private final PipelinerEngine engine;
    private final io.github.amwtke.pipeliner.api.DataSourceRegistry ds;

    public DemoController(Seeder seeder, PipelinerEngine engine, io.github.amwtke.pipeliner.api.DataSourceRegistry ds) {
        this.ds = ds;
        this.seeder = seeder;
        this.engine = engine;
    }

    @PostMapping("/seed")
    public Map<String, Object> seed(@RequestParam(defaultValue = "600") int materials, @RequestParam(defaultValue = "42") long seed) throws Exception {
        return seeder.seed(materials, seed);
    }

    @PostMapping("/rounds")
    public Map<String, String> submit(@RequestParam(defaultValue = "V1") String planVersion) {
        Map<String, String> params = new LinkedHashMap<>();
        params.put("planVersion", planVersion);
        params.put("bizKey", planVersion);
        return Collections.singletonMap("roundId", engine.submit(Demo.GRAPH, planVersion, params));
    }

    /** Fault-injection switch (see runner.Fault): mod > 0 enables, mod = 0 disables; visible to every pod through MySQL. */
    @PostMapping("/fault")
    public Map<String, Object> fault(@RequestParam String pipeline, @RequestParam int mod) throws Exception {
        try (java.sql.Connection c = ds.get("mysql").getConnection(); java.sql.Statement st = c.createStatement()) {
            st.execute("create table if not exists ex_fault(pipeline varchar(32) primary key, modv int not null)");
            if (mod > 0) {
                try (java.sql.PreparedStatement ps = c.prepareStatement("insert into ex_fault values (?,?) on duplicate key update modv = values(modv)")) {
                    ps.setString(1, pipeline);
                    ps.setInt(2, mod);
                    ps.executeUpdate();
                }
            } else {
                try (java.sql.PreparedStatement ps = c.prepareStatement("delete from ex_fault where pipeline = ?")) {
                    ps.setString(1, pipeline);
                    ps.executeUpdate();
                }
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pipeline", pipeline);
        out.put("mod", mod);
        return out;
    }

    @GetMapping("/checksum")
    public Map<String, Object> checksum(@RequestParam(defaultValue = "V1") String planVersion) throws Exception {
        return seeder.checksum(planVersion);
    }
}
