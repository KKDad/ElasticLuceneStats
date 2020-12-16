package org.stapledon.lucene;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class IndexGroup {
    public final String indexGroupName;
    public final List<IndexShard> indices;

    public final Map<String, FieldStatsHolder> fields;

    public long indexGroupSize = 0L;
    public long indexTranslogSize = 0L;
    public long docs = 0L;
    public long deletedDocs = 0L;

    IndexGroup(String indexGroupName)
    {
        this.indexGroupName = indexGroupName;
        this.indices = new ArrayList<>();
        this.fields = new TreeMap<>();
    }

    public void addIndex(IndexShard index) {
        this.indices.add(index);
    }

    public void updateDiskUsage(long indexGroupSize, long indexTranslogSize) {
        this.indexGroupSize +=indexGroupSize;
        this.indexTranslogSize += indexTranslogSize;
    }

    public void updateDocs(long docs, long deletedDocs) {
        this.docs +=docs;
        this.deletedDocs += deletedDocs;
    }

}
