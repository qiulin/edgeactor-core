package com.edgeactor.core;

import com.edgeactor.core.run.Runner;
import com.cloudera.labs.envelope.utils.ConfigUtils;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class EnvelopeBeamMain {

    private static Logger LOG = LoggerFactory.getLogger(EnvelopeBeamMain.class);

    // Entry point to Envelope when submitting directly from spark-submit.
    // Other Java/Scala programs could instead launch an Envelope pipeline by
    // passing their own Config object to Runner#run.
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new RuntimeException("Missing pipeline configuration file argument.");
        } else {
            Path p = Paths.get(args[0]);
            if (Files.notExists(p) || Files.isDirectory(p)) {
                throw new RuntimeException("Can't access pipeline configuration file '" + args[0] + "'.");
            }
        }

        LOG.info("Envelope application started");

        Config config = ConfigUtils.configFromPath(args[0]);
        if (args.length == 2) {
            config = ConfigUtils.applySubstitutions(config, args[1]);
        } else if (args.length > 2) {
            LOG.error("Too many parameters to Envelope application");
        } else {
            config = ConfigUtils.applySubstitutions(config);
        }
        LOG.info("Configuration loaded");

        Runner runner = new Runner();
        runner.run(config);
    }

}
