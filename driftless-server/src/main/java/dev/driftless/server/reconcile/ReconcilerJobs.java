package dev.driftless.server.reconcile;

import dev.driftless.server.config.DriftlessProperties;
import dev.driftless.server.config.PipelineConfig;
import dev.driftless.server.generation.GenerationRepository;
import dev.driftless.server.generation.PipelineStatus;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class ReconcilerJobs {

    private static final Logger log = LoggerFactory.getLogger(ReconcilerJobs.class);

    private final DriftlessProperties props;
    private final GenerationRepository generations;
    private final Reconciler reconciler;

    ReconcilerJobs(DriftlessProperties props, GenerationRepository generations, Reconciler reconciler) {
        this.props = props;
        this.generations = generations;
        this.reconciler = reconciler;
    }

    @Scheduled(fixedDelayString = "${driftless.reconciler.interval:PT5M}", initialDelayString = "${driftless.reconciler.interval:PT5M}")
    @SchedulerLock(name = "reconciler", lockAtMostFor = "PT30M")
    void reconcile() {
        for (PipelineConfig p : props.pipelines()) {
            try {
                if (generations.status(p.name()) == PipelineStatus.ACTIVE) {
                    reconciler.run(p.name());
                }
            } catch (RuntimeException e) {
                log.warn("reconciling {} failed: {}", p.name(), e.getMessage());
            }
        }
    }
}
