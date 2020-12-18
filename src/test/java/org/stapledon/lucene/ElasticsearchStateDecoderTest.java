package org.stapledon.lucene;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ElasticsearchStateDecoderTest {

    @Test
    void generateGroupingsTest() {

        ElasticsearchStateDecoder subject = new ElasticsearchStateDecoder();
        subject.indexHome = "";

        subject.indexMappings.put("<interset_access_rawdata_01p-{2020-10-w42||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());
        subject.indexMappings.put("<interset_access_rawdata_01p-{2020-10-w43||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());
        subject.indexMappings.put("<interset_access_rawdata_01p-{2020-10-w44||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());

        subject.generateGroupings();

        assertEquals(1, subject.INDEX_GROUPS.size());
        assertEquals("interset_access_rawdata_01p", subject.INDEX_GROUPS.keySet().stream().findFirst().get());
    }

    @Test
    void generateGroupingsDifferentTidsTest() {

        ElasticsearchStateDecoder subject = new ElasticsearchStateDecoder();
        subject.indexHome = "";

        subject.indexMappings.put("<interset_access_rawdata_01p-{2020-10-w42||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());
        subject.indexMappings.put("<interset_access_rawdata_0-{2020-10-w43||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());
        subject.indexMappings.put("<interset_access_rawdata_01p-{2020-10-w44||/w{yyyy-MM-'w'ww|UTC}}>", UUID.randomUUID().toString());

        subject.generateGroupings();

        assertEquals(2, subject.INDEX_GROUPS.size());
        assertTrue(subject.INDEX_GROUPS.keySet().stream().filter(p ->p.equalsIgnoreCase("interset_access_rawdata_01p")).findAny().isPresent());
        assertTrue(subject.INDEX_GROUPS.keySet().stream().filter(p ->p.equalsIgnoreCase("interset_access_rawdata_0")).findAny().isPresent());
    }

    @Test
    void generateGroupingsSingleDigitTidTest() {

        ElasticsearchStateDecoder subject = new ElasticsearchStateDecoder();
        subject.indexHome = "";

        subject.indexMappings.put("entity_stats_0_2020-12-11_00_48_35", UUID.randomUUID().toString());
        subject.indexMappings.put("entity_stats_0_2020-12-12_03_26_50", UUID.randomUUID().toString());
        subject.indexMappings.put("entity_stats_0_2021-12-12_20_29_08", UUID.randomUUID().toString());

        subject.generateGroupings();

        assertEquals(1, subject.INDEX_GROUPS.size());
        assertEquals("entity_stats_0", subject.INDEX_GROUPS.keySet().stream().findFirst().get());
    }
}