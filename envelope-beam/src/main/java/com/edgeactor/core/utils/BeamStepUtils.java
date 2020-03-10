package com.edgeactor.core.utils;

import com.cloudera.labs.envelope.component.ComponentFactory;
import com.cloudera.labs.envelope.run.StepState;
import com.edgeactor.core.input.BeamInput;
import com.edgeactor.core.run.BeamStep;
import com.edgeactor.core.run.DataBeamStep;
import com.edgeactor.core.run.BeamRunner;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.edgeactor.core.run.UnifiedDataStep;
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

        Set<String> stepNames = config.getObject(BeamRunner.STEPS_SECTION_CONFIG).keySet();
        for (String stepName : stepNames) {
            Config stepConfig = config.getConfig(BeamRunner.STEPS_SECTION_CONFIG).getConfig(stepName);

            BeamStep step;

            if (!stepConfig.hasPath(BeamRunner.TYPE_PROPERTY) ||
                    stepConfig.getString(BeamRunner.TYPE_PROPERTY).equals(BeamRunner.DATA_TYPE)) {

                if (stepConfig.hasPath(DataBeamStep.INPUT_TYPE)) {
                    Config stepInputConfig = stepConfig.getConfig(DataBeamStep.INPUT_TYPE);
                    BeamInput stepInput = ComponentFactory.create(BeamInput.class, stepInputConfig, false);

                    if (stepInput instanceof BeamInput) {
                        LOG.debug("Adding Bound step: " + stepName);
                        step = new UnifiedDataStep(stepName);
                    } else {
                        throw new RuntimeException("Invalid step input sub-class for: " + stepName);
                    }
                } else {

                    LOG.debug("Adding batch step: " + stepName);
                    step = new UnifiedDataStep(stepName);
                }
//            } else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.LOOP_TYPE)) {
//                step = null;
                // TODO
//            } else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.TASK_TYPE)) {
                // TODO
//            } else if (stepConfig.getString(Runner.TYPE_PROPERTY).equals(Runner.DECISION_TYPE)) {
                // TODO
            } else {
                throw new RuntimeException("Unknown step type: " + stepConfig.getString(com.cloudera.labs.envelope.run.Runner.TYPE_PROPERTY));
            }

            if (configure) {
                step.configure(stepConfig);
                LOG.debug("With configuration: " + stepConfig);
            }
            steps.add(step);
        }

        if (notify) {

        }
        return steps;
    }

    public static boolean allStepsSubmitted(Set<BeamStep> steps) {
        for (BeamStep step : steps) {
            if (step.getState() == StepState.WAITING) {
                return false;
            }
        }

        return true;
    }

    public static boolean allStepsFinished(Set<BeamStep> steps) {
        for (BeamStep step : steps) {
            if (step.getState() != StepState.FINISHED) {
                return false;
            }
        }

        return true;
    }

}
