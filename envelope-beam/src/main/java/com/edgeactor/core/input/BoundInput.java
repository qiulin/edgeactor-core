package com.edgeactor.core.input;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface BoundInput extends BeamInput{

    PCollection<Row> read() throws Exception;
}
