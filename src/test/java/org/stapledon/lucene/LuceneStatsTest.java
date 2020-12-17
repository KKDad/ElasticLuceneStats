package org.stapledon.lucene;

import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LuceneStatsTest {

    @Test
    void runTestExact() {
        CommandLine options = mock(CommandLine.class);
        when(options.getOptionValue(StartupUtils.OPTION_INDEX_DIRECTORY)).thenReturn("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");

        ElasticLuceneStats subject = new ElasticLuceneStats(options);
        subject.process();
    }

    @Test
    void runTestSampled() {
        CommandLine options = mock(CommandLine.class);
        when(options.getOptionValue(StartupUtils.OPTION_INDEX_DIRECTORY)).thenReturn("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
        when(options.getOptionValue(StartupUtils.OPTION_NUMBER_OF_SAMPLES, StartupUtils.DEFAULT_SAMPLE_SIZE)).thenReturn("250");
        when(options.hasOption(StartupUtils.OPTION_SAMPLE)).thenReturn(true);

        ElasticLuceneStats subject = new ElasticLuceneStats(options);
        subject.process();
    }

    @Test
    void runTestIncludeDoc() {
        CommandLine options = mock(CommandLine.class);
        when(options.getOptionValue(StartupUtils.OPTION_INDEX_DIRECTORY)).thenReturn("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
        when(options.hasOption(StartupUtils.OPTION_DOC)).thenReturn(true);

        ElasticLuceneStats subject = new ElasticLuceneStats(options);
        subject.process();
    }



}