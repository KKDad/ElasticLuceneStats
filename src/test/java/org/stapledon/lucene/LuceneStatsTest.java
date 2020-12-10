package org.stapledon.lucene;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class LuceneStatsTest {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneStatsTest.class);


    @BeforeEach
    void setUp() {
    }

    @Test
    void runTest() {
        LuceneStats subject = new LuceneStats();
        //subject.run("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\indices\\0uvamnA_S9-wfaO2iQECJQ\\0\\index");
        //subject.run("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\indices\\5fY3jo5ATXqPAhj_AzWJ-g\\0\\index");
        //subject.run("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\indices\\5E2qT6B9RyidvKdLEGqkUA\\0\\index");
        subject.process("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
    }


}