package com.edgeactor.core.run;

import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.security.TokenStoreManager;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.cloudera.labs.envelope.utils.StepUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;

public class Runner {

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

        Set<Step> steps = StepUtils.extractSteps(config, true, true);

    }

    private void validateConfigurations(Config config) {
        // TODO
    }
}
