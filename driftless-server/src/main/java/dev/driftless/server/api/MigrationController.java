package dev.driftless.server.api;

import dev.driftless.server.generation.Generation;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.MigrationService;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
class MigrationController {

    private final MigrationService migrations;
    private final GenerationRepository generations;

    MigrationController(MigrationService migrations, GenerationRepository generations) {
        this.migrations = migrations;
        this.generations = generations;
    }

    @PostMapping("/v1/pipelines/{name}/migrations")
    @ResponseStatus(HttpStatus.CREATED)
    Generation start(@PathVariable String name) {
        return migrations.start(name);
    }

    /** Each step answers with the generation that is live afterwards. */
    @PostMapping("/v1/pipelines/{name}/migrations/cutover")
    Generation cutover(@PathVariable String name) {
        migrations.cutover(name);
        return live(name);
    }

    @PostMapping("/v1/pipelines/{name}/migrations/rollback")
    Generation rollback(@PathVariable String name) {
        migrations.rollback(name);
        return live(name);
    }

    @PostMapping("/v1/pipelines/{name}/migrations/finalize")
    Generation finalizeMigration(@PathVariable String name) {
        migrations.finalizeMigration(name);
        return live(name);
    }

    private Generation live(String name) {
        return generations.live(name).orElseThrow(() -> new ConflictException("pipeline " + name + " has no live generation"));
    }
}
