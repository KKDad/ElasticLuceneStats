package org.stapledon.lucene;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartupUtils {
    private static final Logger LOG = LoggerFactory.getLogger(StartupUtils.class);
    private static final Integer ERROR_STATUS = 1;


    public static CommandLine parseOptions(String[] args) {
        Options options = new Options();

        Option option = new Option("c", "configFile", true, "path to configuration yaml");
        //option.setRequired(true);
        options.addOption(option);

        option = new Option("d", "indexDirectory", true, "directory containing lucene index");
        option.setRequired(true);
        options.addOption(option);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            LOG.error(e.getLocalizedMessage());
            formatter.printHelp("LuceneStats", options);
            System.exit(ERROR_STATUS);
        }
        return null;
    }
}
