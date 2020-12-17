package org.stapledon.lucene;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StartupUtils {
    private static final Logger LOG = LoggerFactory.getLogger(StartupUtils.class);
    private static final Integer ERROR_STATUS = 1;

    public static final String DEFAULT_SAMPLE_SIZE = "10000";

    public static final String OPTION_INDEX_DIRECTORY = "indexDirectory";
    public static final String OPTION_SAMPLE = "sample";
    public static final String OPTION_DOC = "doc";
    public static final String OPTION_NUMBER_OF_SAMPLES = "sampleSize";


    public static CommandLine parseOptions(String[] args) {
        Options options = new Options();

        // Required - directory to the ElasticSearch index
        Option option = new Option("d", OPTION_INDEX_DIRECTORY, true, "Directory containing elasticsearch index");
        option.setRequired(true);
        options.addOption(option);

        option = new Option("s", OPTION_SAMPLE, false, "Sample the lucene index and produce an estimated size for stored fields.");
        options.addOption(option);

        option = new Option("n", OPTION_NUMBER_OF_SAMPLES, true, "Number of documents to sample per segment, if sampling is enabled. Defaults to " + DEFAULT_SAMPLE_SIZE);
        options.addOption(option);


        option = new Option("i", OPTION_DOC, false, "Include a random document for stored fields.");
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
