package com.edgeactor.core.input;

import com.cloudera.labs.envelope.input.Input;

public interface BeamInput extends Input {

    PCollection<Row> read() throws Exception;
}
