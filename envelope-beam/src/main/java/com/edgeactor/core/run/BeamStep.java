package com.edgeactor.core.run;

import com.cloudera.labs.envelope.run.Step;

public abstract class BeamStep extends Step {

    public BeamStep(String name) {
        super(name);
    }
}
