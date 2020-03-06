package com.edgeactor.core.utils;

import com.edgeactor.core.run.BeamStep;
import com.edgeactor.core.run.Runner;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.google.api.client.util.Sets;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

public class BeamStepUtils {

    private static final Logger LOG = LoggerFactory.getLogger(StepUtils.class);

    public static Set<BeamStep> extractSteps(Config config, boolean configure, boolean notify) {
        LOG.debug("Starting extracting steps");

        long startTime = System.nanoTime();
        Set<BeamStep> steps = Sets.newHashSet();

        Set<String> stepNames = config.getObject(Runner.STEPS_SECTION_CONFIG).keySet();
        for (String stepName : stepNames) {
            Config stepConfig = config.getConfig(Runner.STEPS_SECTION_CONFIG).getConfig(stepName);

            BeamStep step;

            if (!stepConfig.hasPath(Runner.TYPE_PROPERTY) ||
                    stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.DATA_TYPE)) {

                if (stepConfig.hasPath())

            }
        }
    }
}
