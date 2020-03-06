package com.edgeactor.core.output;

import com.cloudera.labs.envelope.plan.MutationType;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import scala.Tuple2;

import java.util.List;
import java.util.Set;

public interface BoundOutput extends BeamOutput {

    Set<MutationType> getSupportedBulkMutationTypes();

    void applyBulkMutations(List<Tuple2<MutationType, PCollection<Row>>> planned);
}
