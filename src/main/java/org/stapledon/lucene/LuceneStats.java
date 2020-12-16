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
import java.util.*;
import java.util.stream.Stream;

public class LuceneStats {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneStats.class);

    public static void main(String[] args) {
        LuceneStats ls = new LuceneStats();
        CommandLine cmd = StartupUtils.parseOptions(args);
        ls.process(cmd.getOptionValue("indexDirectory"));
    }

    public final Map<String, StatsHolder> FIELD_STATS = new TreeMap<>();
    private long ES_INDEX_SIZE = 0L;
    private long INDEX_GROUP_SIZE = 0L;
    private long INDEX_TRANSLOG_SIZE = 0L;
    private long CALCULATED_GROUP_SIZE = 0L;


    public void loadIndexStats(Index index) {
        try {
            this.INDEX_GROUP_SIZE += getDirectorySize(Paths.get(index.getIndexDirectoryName()));
            this.INDEX_TRANSLOG_SIZE += getDirectorySize(Paths.get(index.getTransLogDirectoryName()));
            Directory indexDirectory = FSDirectory.open(Paths.get(index.getIndexDirectoryName()));
            IndexReader indexReader = DirectoryReader.open(indexDirectory);
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
                for (FieldInfo field : reader.getFieldInfos()) {
                    FIELD_STATS.putIfAbsent(field.name, new StatsHolder(field.name));
                    Terms terms = reader.terms(field.name);
                    if (terms instanceof FieldReader) {
                        Stats fieldStats = ((FieldReader) terms).getStats();
                        FIELD_STATS.get(field.name).accumulateStats(fieldStats);
                    }
                    long docValues = statsStoredFieldVisitor.STATS.getOrDefault(field.name, 0L);
                    FIELD_STATS.get(field.name).accumulateStoredFieldBytes(docValues);
                    index.accumulateDocs(reader.numDocs(), reader.numDeletedDocs());
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
                    .mapToLong(LuceneStats::fileSize)
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


    public void process(String directory) {
        ElasticsearchStateDecoder dm = new ElasticsearchStateDecoder(directory);
        String indexHome = directory.replace("_state", "indices");
        ES_INDEX_SIZE = getDirectorySize(Paths.get(indexHome));
        dm.INDEX_GROUPS.forEach((s, indices) -> {
            for (Index indexItem : indices) {
                loadIndexStats(indexItem);
            }
            LOG.info("Processing Index Group: {}", s);
            FIELD_STATS.forEach((key, value) -> { CALCULATED_GROUP_SIZE += value.getTotal(); });
            FIELD_STATS.forEach((key, value) -> {
                value.calculate(CALCULATED_GROUP_SIZE, INDEX_TRANSLOG_SIZE);
                LOG.info("  -> {}", value);
            });
            LOG.info("Index Group: {}, {}% of node total, Index: {} bytes, Total bytes (Uncompressed): {},", s, String.format("%2.2f", ((double)INDEX_GROUP_SIZE/ES_INDEX_SIZE)*100), String.format("%,-2d", INDEX_GROUP_SIZE), String.format("%,-2d", CALCULATED_GROUP_SIZE));
            LOG.info("--------------------------------------------------\n\n");

            FIELD_STATS.clear();
            INDEX_GROUP_SIZE = 0L;
            INDEX_TRANSLOG_SIZE = 0L;
            CALCULATED_GROUP_SIZE = 0L;
        });
    }

    class StatsHolder {
        String name;

        // Stored Documents
        long storedFieldBytes;

        // Term Stats
        long indexNumBytes;
        long totalTermBytes;
        long totalBlockSuffixBytes;
        long totalUncompressedBlockSuffixBytes;
        long totalBlockStatsBytes;
        long totalBlockOtherBytes;

        long indexTotalBytes;
        long indexTotalTransLogBytes;

        // Total
        double percentage;
        Long getTotal() {
            return storedFieldBytes + indexNumBytes + totalTermBytes + totalBlockSuffixBytes + totalUncompressedBlockSuffixBytes + totalBlockStatsBytes + totalBlockOtherBytes;
        }

        public void calculate(long indexTotalBytes, long indexTotalTransLogBytes)
        {
            this.indexTotalBytes = indexTotalBytes;
            this.indexTotalTransLogBytes = indexTotalTransLogBytes;
            percentage = ((double)getTotal())  / indexTotalBytes * 100;
        }

        public StatsHolder(String name) {
            this.name = name;
        }

        public void accumulateStats(Stats stats) {
            indexNumBytes += stats.indexNumBytes;
            totalTermBytes += stats.totalTermBytes;
            totalBlockSuffixBytes += stats.totalBlockSuffixBytes;
            totalUncompressedBlockSuffixBytes += stats.totalUncompressedBlockSuffixBytes;
            totalBlockStatsBytes += stats.totalBlockStatsBytes;
            totalBlockOtherBytes += stats.totalBlockOtherBytes;
        }

        public void accumulateStoredFieldBytes(Long storedFieldBytes) {
            this.storedFieldBytes += storedFieldBytes;
        }

        @Override
        public String toString() {
            return String.format("%-35s (%2.2f%%), Stored=%,d, IndexBytes=%,d, TermBytes=%,d, BlockSuffixBytes=%,d, UncompressedBlockSuffixBytes=%,d, BlockStatsBytes=%,d, BlockOtherBytes=%,d, FieldTotal=%,d",
                    name, percentage, storedFieldBytes, indexNumBytes, totalTermBytes, totalBlockSuffixBytes, totalUncompressedBlockSuffixBytes, totalBlockStatsBytes, totalBlockOtherBytes, getTotal());
        }
    }

}
