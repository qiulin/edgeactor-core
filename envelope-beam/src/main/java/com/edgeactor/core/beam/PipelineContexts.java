package com.edgeactor.core.beam;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public enum PipelineContexts {
    INSTANCE;

    public static final String PIPELINE_SECTION_PREFIX = "pipeline";
    public static final String BEAM_SECTION_PREFIX = "beam";
    public static final String EDGEACTOR_SECTION_PREFIX = "edgeactor";
    private Config config = ConfigFactory.empty();
    private Pipeline pipeline;
//    private PipelineOptions pipelineOptions;

    public static void initialize(Config config) {
        INSTANCE.config = config.hasPath(PIPELINE_SECTION_PREFIX) ?
                config.getConfig(PIPELINE_SECTION_PREFIX) : ConfigFactory.empty();
        closePipeline();
        getOrCreatePipeline();

    }

    private static synchronized Pipeline getOrCreatePipeline() {
        if (!hasPipeline()) {
            startPipeline();
        }

        return INSTANCE.pipeline;
    }

    private static synchronized void startPipeline() {
        PipelineOptions options;
        // TODO: get beam config
//        if (!INSTANCE.config.hasPipelinesPath(BEAM_SECTION_PREFIX)) {
//            options = PipelineOptionsFactory.create();
//        } else {
//            options = PipelineOptionsFactory.(INSTANCE.config.getStringList(BEAM_SECTION_PREFIX));
//        }
        options = PipelineOptionsFactory.create();
        INSTANCE.pipeline = Pipeline.create(options);
    }

    public static synchronized void closePipeline() {
        // TODO
    }

    private static synchronized boolean hasPipeline() {
        return INSTANCE.pipeline != null;
    }

}
