package com.edgeactor.core.run;

import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public abstract class DataBeamStep
        extends BeamStep implements ProvidesValidations, InstantiatesComponents {

    public static final String INPUT_TYPE = "input";
    public static final String DERIVER_TYPE = "deriver";
    public static final String PLANNER_TYPE = "planner";
    public static final String OUTPUT_TYPE = "output";
    public static final String PARTITIONER_TYPE = "partitioner";
    public static final String CACHE_ENABLED_PROPERTY = "cache.enabled";
    public static final String CACHE_STORAGE_LEVEL_PROPERTY = "cache.storage.level";
    public static final String SMALL_HINT_PROPERTY = "hint.small";
    public static final String PRINT_SCHEMA_ENABLED_PROPERTY = "print.schema.enabled";
    public static final String PRINT_DATA_ENABLED_PROPERTY = "print.data.enabled";
    public static final String PRINT_DATA_LIMIT_PROPERTY = "print.data.limit";

    private PCollection<Row> data;

    private PCollection<Row> output;

    public DataBeamStep(String name) {
        super(name);
    }

    public PCollection<Row> getData() {
        return data;
    }

    public void setData(PCollection<Row> data) {
        this.data = data;
    }

    public PCollection<Row> getOutput() {
        return output;
    }

    public void setOutput(PCollection<Row> output) {
        this.output = output;
    }
}
