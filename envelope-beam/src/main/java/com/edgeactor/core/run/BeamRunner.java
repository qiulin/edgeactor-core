package com.edgeactor.core.run;

import com.cloudera.labs.envelope.run.StepState;
import com.cloudera.labs.envelope.security.TokenStoreManager;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.edgeactor.core.beam.PipelineContexts;
import com.edgeactor.core.utils.BeamStepUtils;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BeamRunner {

    public static final String STEPS_SECTION_CONFIG = "steps";
    public static final String TYPE_PROPERTY = "type";
    public static final String DATA_TYPE = "data";
    public static final String LOOP_TYPE = "loop";
    public static final String DECISION_TYPE = "decision";
    public static final String TASK_TYPE = "task";
    public static final String UDFS_SECTION_CONFIG = "udfs";
    public static final String UDFS_NAME = "name";
    public static final String UDFS_CLASS = "class";
    public static final String EVENT_HANDLERS_CONFIG = "event-handlers";
    public static final String CONFIG_LOADER_PROPERTY = "config-loader";
    public static final String PIPELINE_THREADS_PROPERTY = "application.pipeline.threads";

    private Config baseConfig;
    private ExecutorService threadPool;
    private TokenStoreManager tokenStoreManager;


    private static Logger LOG = LoggerFactory.getLogger(com.cloudera.labs.envelope.run.Runner.class);

    public void run(Config config) throws Exception {
        this.baseConfig = config;
        config = ConfigUtils.mergeLoadedConfiguration(config);

        validateConfigurations(config);

        Set<BeamStep> steps = BeamStepUtils.extractSteps(config, true, true);

        // Beam 统一了 Stream / Batch
        PipelineContexts.initialize(config);

        try {
            runStep(steps);
        } catch (Exception e) {
            throw e;
        } finally {
            PipelineContexts.closePipeline();
        }

    }


    private void runStep(Set<BeamStep> steps) throws Exception {
        if (steps.isEmpty()) {
            return;
        }

        LOG.debug("Started for steps: {}", StepUtils.stepNamesAsString(steps));
        Set<Future<Void>> offMainThreadSteps = Sets.newHashSet();
        Set<BeamStep> refactoredSteps = null;
        Map<String, StepState> previousStepStates = null;

        while (!BeamStepUtils.allStepsSubmitted(steps)) {
            LOG.debug("Not all steps have been submitted");

            Set<BeamStep> newSteps = Sets.newHashSet();
            for (final BeamStep step : steps) {
                LOG.debug("Looking into step: " + step.getName());
                if (step instanceof UnifiedDataStep) {
                    UnifiedDataStep uStep = (UnifiedDataStep) step;

                    if (uStep.getState() == StepState.WAITING) {
                        LOG.debug("Step has not been submitted");

                    }
                }
            }

        }
    }

    private void initializeThreadPool(Config config) {
        if (config.hasPath(PIPELINE_THREADS_PROPERTY)) {
            threadPool = Executors.newFixedThreadPool(config.getInt(PIPELINE_THREADS_PROPERTY));
        } else {
            threadPool = Executors.newFixedThreadPool(20);
        }
    }

    private void shutdownThreadPool() {
        threadPool.shutdown();
    }

    private void validateConfigurations(Config config) {
        // TODO
    }
}
