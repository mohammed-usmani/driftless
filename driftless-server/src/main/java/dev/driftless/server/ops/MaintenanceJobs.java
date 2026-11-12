package dev.driftless.server.ops;

import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class MaintenanceJobs {

    private final JdbcTemplate jdbc;

    MaintenanceJobs(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /** recent_change is a window for the console, not a log: keep the newest 1000 per pipeline. */
    @Scheduled(fixedDelay = 60_000, initialDelay = 60_000)
    @SchedulerLock(name = "trim-recent-changes", lockAtMostFor = "PT5M")
    void trimRecentChanges() {
        jdbc.update("""
                DELETE FROM recent_change WHERE id IN (
                    SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY pipeline ORDER BY id DESC) AS n FROM recent_change) x
                    WHERE n > 1000)""");
    }

    @Scheduled(cron = "0 17 3 * * *")
    @SchedulerLock(name = "expire-embedding-cache", lockAtMostFor = "PT1H")
    void expireEmbeddingCache() {
        jdbc.update("DELETE FROM embedding_cache WHERE last_used_at < now() - interval '30 days'");
    }
}
