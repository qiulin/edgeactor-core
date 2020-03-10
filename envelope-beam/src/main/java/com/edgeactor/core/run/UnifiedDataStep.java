package com.edgeactor.core.run;

import com.cloudera.labs.envelope.component.InstantiatedComponent;
import com.cloudera.labs.envelope.component.InstantiatesComponents;
import com.cloudera.labs.envelope.run.Step;
import com.cloudera.labs.envelope.validate.ProvidesValidations;
import com.typesafe.config.Config;

import java.util.Set;

public class UnifiedDataStep extends DataBeamStep implements ProvidesValidations, InstantiatesComponents {

    public UnifiedDataStep(String name) {
        super(name);
    }

    @Override
    public void configure(Config config) {
        super.configure(config);
    }

    public void submit(Set<BeamStep> dependencySteps) throws Exception {

    }

    @Override
    public Set<InstantiatedComponent> getComponents(Config config, boolean configure) throws Exception {
        return null;
    }

    @Override
    public Step copy() {
        return null;
    }
}
