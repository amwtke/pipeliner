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

    public DemoController(Seeder seeder, PipelinerEngine engine) {
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

    @GetMapping("/checksum")
    public Map<String, Object> checksum(@RequestParam(defaultValue = "V1") String planVersion) throws Exception {
        return seeder.checksum(planVersion);
    }
}
