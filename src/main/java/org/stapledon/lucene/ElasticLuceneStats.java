package org.stapledon.lucene;

import org.apache.commons.cli.CommandLine;
import org.apache.lucene.codecs.blocktree.FieldReader;
import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class ElasticLuceneStats {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticLuceneStats.class);
    public static final String DISK_BYTES = "%,15d bytes";
    public static final String SECTION_SEPARATOR = "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------";

    public static void main(String[] args)
    {
        ElasticLuceneStats ls = new ElasticLuceneStats();
        CommandLine cmd = StartupUtils.parseOptions(args);
        ls.processNode(cmd.getOptionValue("indexDirectory"));
    }


    private void loadIndexStats(IndexGroup group, IndexShard index) {
        try {
            // Get the size of this index and add it to the Index Group
            long indexGroupSize = getDirectorySize(Paths.get(index.getIndexDirectoryName()));
            long indexTranslogSize = getDirectorySize(Paths.get(index.getTransLogDirectoryName()));
            index.updateDiskUsage(indexGroupSize, indexTranslogSize);
            group.updateDiskUsage(indexGroupSize, indexTranslogSize);

            Directory indexDirectory = FSDirectory.open(Paths.get(index.getIndexDirectoryName()));
            try (IndexReader indexReader = DirectoryReader.open(indexDirectory)) {
                LOG.debug("Number of Docs: {}", indexReader.numDocs());
                for (LeafReaderContext context : indexReader.leaves()) {
                    // Visit all of the documents and calculate the size of the stored fields
                    StatsStoredFieldVisitor statsStoredFieldVisitor = new StatsStoredFieldVisitor();
                    LeafReader reader = FilterLeafReader.unwrap(context.reader());
                    int i = 0;
                    while (i < reader.maxDoc()) {
                        reader.document(i, statsStoredFieldVisitor);
                        i++;
                    }
                    // Visit all of the Fields and get the statistics for them
                    for (FieldInfo field : reader.getFieldInfos()) {
                        group.fields.putIfAbsent(field.name, new FieldStatsHolder(field.name, field.getIndexOptions()));
                        Terms terms = reader.terms(field.name);
                        if (terms instanceof FieldReader) {
                            Stats fieldStats = ((FieldReader) terms).getStats();
                            group.fields.get(field.name).accumulateStats(fieldStats);
                        }
                        long docValues = statsStoredFieldVisitor.STATS.getOrDefault(field.name, 0L);
                        group.fields.get(field.name).accumulateStoredFieldBytes(docValues);
                    }
                    // Accumulate the number of docs and deleted docs for this segment
                    index.updateDocs(reader.numDocs(), reader.numDeletedDocs());
                    group.updateDocs(reader.numDocs(), reader.numDeletedDocs());
                }
            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    private long getDirectorySize(Path path) {
        long size = 0;
        try (Stream<Path> walk = Files.walk(path)) {
            size = walk
                    .filter(Files::isRegularFile)
                    .mapToLong(ElasticLuceneStats::fileSize)
                    .sum();
        } catch (IOException e) {
            LOG.error("{}", e.getLocalizedMessage(), e);
        }
        return size;
    }

    private static long fileSize(Path targetPath) {
        try {
            return Files.size(targetPath);
        } catch (IOException e) {
            LOG.error("Failed to get size of {}: {}", targetPath, e.getLocalizedMessage(), e);
            return 0L;
        }
    }


    public void processNode(String esStateDirectory) {
        if (!esStateDirectory.toLowerCase().endsWith("_state")) {
            LOG.error("This doesn't look like a Elasticsearch node state directory.");
            LOG.error("Expected something like: D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
            return;
        }
        ElasticsearchStateDecoder dm = new ElasticsearchStateDecoder(esStateDirectory);
        if (dm.INDEX_GROUPS.size() == 0) {
            LOG.error("No index groups loaded.");
            return;
        }
        String esIndexDirectory = esStateDirectory.replace("_state", "indices");
        long esIndexSize = getDirectorySize(Paths.get(esIndexDirectory));
        dm.INDEX_GROUPS.forEach((indexGroupName, indexGroup) -> {

            // Display Segment Details
            indexGroup.indices.forEach(i -> loadIndexStats(indexGroup, i));
            indexGroup.calculate();

            // Load the Statistics for each segment
            indexGroup.fields.forEach((key, fieldStats) -> fieldStats.calculate(indexGroup.totalCalculatedSize));

            LOG.info("Index Group: {}", indexGroupName);
            LOG.info(SECTION_SEPARATOR);
            for (IndexShard index : indexGroup.indices) {
                LOG.info("  -> {}", index);
            }
            LOG.info(SECTION_SEPARATOR);


            LOG.info("Index Statistics: {}", indexGroupName);
            LOG.info(" - # of Documents    : {}", String.format("%,15d", indexGroup.docs));
            LOG.info(" - # of Deleted Docs : {}", String.format("%,15d", indexGroup.deletedDocs));
            LOG.info(" - Overall Percentage: {}", String.format("%15.2f %%", ((double) indexGroup.totalDiskSize / esIndexSize)*100));
            LOG.info(" - Lucene Index      : {}", String.format(DISK_BYTES, indexGroup.totalDiskSize));
            LOG.info(" - Lucene TransLog   : {}", String.format(DISK_BYTES, indexGroup.totalTransLogSize));
            LOG.info(" - Total Uncompressed: {}", String.format(DISK_BYTES, indexGroup.totalDiskSize));
            LOG.info(SECTION_SEPARATOR);
            if (indexGroup.fields.size() == 0)
                LOG.info("No Records");
            else
                indexGroup.fields.forEach((key, fieldStats) -> LOG.info("  -> {}", fieldStats));
            LOG.info("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n\n");
        });
    }
}
