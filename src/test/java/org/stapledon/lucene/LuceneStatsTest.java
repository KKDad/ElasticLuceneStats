package org.stapledon.lucene;

import org.junit.jupiter.api.Test;

class LuceneStatsTest {

    @Test
    void runTest() {
        ElasticLuceneStats subject = new ElasticLuceneStats();
        subject.processNode("D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
    }
}