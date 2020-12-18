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

    public final CommandLine options;

    public ElasticLuceneStats(CommandLine options) {
        this.options = options;
    }

    public static void main(String[] args)
    {
        CommandLine options = StartupUtils.parseOptions(args);
        ElasticLuceneStats ls = new ElasticLuceneStats(options);
        ls.process();
    }


    private void loadIndexStats(IndexGroup group, IndexShard index) {
        try {
            // Get the size of this index and add it to the Index Group
            long indexGroupSize = getDirectorySize(Paths.get(index.getIndexDirectoryName()));
            long indexTranslogSize = getDirectorySize(Paths.get(index.getTransLogDirectoryName()));
            index.updateDiskUsage(indexGroupSize, indexTranslogSize);
            group.updateDiskUsage(indexGroupSize, indexTranslogSize);

            Directory indexDirectory = FSDirectory.open(Paths.get(index.getIndexDirectoryName()));
            if (indexDirectory.listAll().length == 0) {
                LOG.error("No Lucene segments located in {}", index.getIndexDirectoryName());
                return;
            }
            try (IndexReader indexReader = DirectoryReader.open(indexDirectory)) {
                LOG.debug("{} has {} segments to process", index.getIndexShortName(), indexReader.leaves().size());
                for (LeafReaderContext context : indexReader.leaves()) {

                    // Visit all of the documents and calculate the size of the stored fields
                    boolean estimate = this.options.hasOption(StartupUtils.OPTION_SAMPLE);
                    StatsStoredFieldVisitor statsStoredFieldVisitor = new StatsStoredFieldVisitor();
                    LeafReader reader = FilterLeafReader.unwrap(context.reader());
                    int i = 0;
                    LOG.debug("  -> Processing segment {} with {} documents", context.ord, reader.numDocs());
                    long segmentLimit = this.options.hasOption(StartupUtils.OPTION_SAMPLE) ? Long.parseLong(this.options.getOptionValue(StartupUtils.OPTION_NUMBER_OF_SAMPLES, StartupUtils.DEFAULT_SAMPLE_SIZE)) : Long.MAX_VALUE;
                    segmentLimit = Math.min(segmentLimit, reader.maxDoc());
                    while (i < segmentLimit) {
                        reader.document(i, statsStoredFieldVisitor);
                        i++;
                    }
                    // Visit all of the Fields and get the statistics for them
                    LOG.debug("  -> Processing {} fields", reader.getFieldInfos().size());
                    for (FieldInfo field : reader.getFieldInfos()) {
                        group.fields.putIfAbsent(field.name, new FieldStatsHolder(field.name, field.getIndexOptions()));
                        Terms terms = reader.terms(field.name);
                        if (terms instanceof FieldReader) {
                            Stats fieldStats = ((FieldReader) terms).getStats();
                            group.fields.get(field.name).accumulateStats(fieldStats);
                        }
                        // Adjust docValues if it's an estimated value
                        long docValues = statsStoredFieldVisitor.STATS.getOrDefault(field.name, 0L);
                        if (estimate) {
                            docValues = new Double((docValues / segmentLimit) * reader.maxDoc()).longValue();
                        }
                        group.fields.get(field.name).accumulateStoredFieldBytes(docValues);
                        group.fields.get(field.name).accumulateSample(statsStoredFieldVisitor.SAMPLE.getOrDefault(field.name, null));
                    }
                    // Accumulate the number of docs and deleted docs for this segment
                    index.updateDocs(reader.numDocs(), reader.numDeletedDocs());
                    group.updateDocs(reader.numDocs(), reader.numDeletedDocs());
                }
            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage());
        }
    }

    private long getDirectorySize(Path path) {
        if (!path.toFile().exists())
            return 0L;

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


    public void process() {
        String esStateDirectory = this.options.getOptionValue(StartupUtils.OPTION_INDEX_DIRECTORY);
        if (!esStateDirectory.toLowerCase().endsWith("_state")) {
            LOG.error("This doesn't look like a Elasticsearch node state directory.");
            LOG.error("Expected something like: D:\\elasticsearch\\ag16-cdf-single.ad.interset.com\\nodes\\0\\_state");
            return;
        }
        ElasticsearchStateDecoder dm = new ElasticsearchStateDecoder();
        dm.decode(esStateDirectory);
        dm.generateGroupings();
        if (dm.INDEX_GROUPS.size() == 0) {
            LOG.error("No index groups loaded.");
            return;
        }
        String esIndexDirectory = esStateDirectory.replace("_state", "indices");
        long esIndexSize = getDirectorySize(Paths.get(esIndexDirectory));
        dm.INDEX_GROUPS.forEach((indexGroupName, indexGroup) -> {

            // Load the Statistics for each segment
            LOG.debug("Index Group: {}", indexGroupName);
            indexGroup.indices.forEach(i -> loadIndexStats(indexGroup, i));
            indexGroup.calculate();

            indexGroup.fields.forEach((key, fieldStats) -> fieldStats.calculate(indexGroup.totalCalculatedSize));
        });
        LOG.info("{}\n\n", SECTION_SEPARATOR);
        LOG.info("Index Groups");
        LOG.info(SECTION_SEPARATOR);
        dm.INDEX_GROUPS.forEach((indexGroupName, indexGroup) -> {
            LOG.info(String.format("Index Group: %-33s  Percentage: %6.2f%%,  Documents: %,18d,  Size: %,20d",
                    indexGroupName,
                    ((double) indexGroup.totalDiskSize / esIndexSize)*100,
                    indexGroup.docs,
                    indexGroup.totalDiskSize));

        });
        LOG.info("{}\n\n", SECTION_SEPARATOR);
        dm.INDEX_GROUPS.forEach((indexGroupName, indexGroup) -> {

            // Display Segment Details
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
            else {
                indexGroup.fields.forEach((key, fieldStats) -> LOG.info("  -> {}", fieldStats));
                if (options.hasOption(StartupUtils.OPTION_DOC)) {
                    LOG.info(SECTION_SEPARATOR);
                    LOG.info("Sample Docs:");
                    indexGroup.fields.forEach((key, fieldStats) -> {
                            if (fieldStats.sampleDocs.size() > 0)
                                LOG.info("  {} -> {}", fieldStats.name, fieldStats.sampleDocs.get(0));
                    });
                }
            }
            LOG.info("{}\n\n", SECTION_SEPARATOR);
        });
    }
}
