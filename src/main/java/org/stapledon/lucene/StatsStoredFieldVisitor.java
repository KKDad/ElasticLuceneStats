package org.stapledon.lucene;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;

import java.io.IOException;
import java.util.TreeMap;

public class StatsStoredFieldVisitor extends StoredFieldVisitor {

    public final TreeMap<String, Long> STATS = new TreeMap<>();

    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
        STATS.putIfAbsent(fieldInfo.name, 0L);
        return Status.YES;
    }

    public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+value.length);
    }

    /** Process a string field; the provided byte[] value is a UTF-8 encoded string value. */
    public void stringField(FieldInfo fieldInfo, byte[] value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+value.length);
    }

    /** Process a int numeric field. */
    public void intField(FieldInfo fieldInfo, int value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+1);
    }

    /** Process a long numeric field. */
    public void longField(FieldInfo fieldInfo, long value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+1);
    }

    /** Process a float numeric field. */
    public void floatField(FieldInfo fieldInfo, float value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+1);
    }

    /** Process a double numeric field. */
    public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
        STATS.put(fieldInfo.name, STATS.get(fieldInfo.name)+1);
    }
}
