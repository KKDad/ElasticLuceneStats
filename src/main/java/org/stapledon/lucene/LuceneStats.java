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
        ls.loadIndexStats(cmd.getOptionValue("indexDirectory"));
    }

    public final Map<String, List<StatsHolder>> FIELD_STATS = new TreeMap<>();
    public long INDEX_GROUP_SIZE = 0L;
    public long CALCULATED_GROUP_SIZE = 0L;


    public void loadIndexStats(String directory) {
        try {
            this.INDEX_GROUP_SIZE += getDirectorySize(Paths.get(directory));
            Directory indexDirectory = FSDirectory.open(Paths.get(directory));
            IndexReader indexReader = DirectoryReader.open(indexDirectory);
            LOG.debug("Number of Docs: {}", indexReader.numDocs());
            for (LeafReaderContext context : indexReader.leaves()) {
                // Visit all of the documents and calculate the size of the stored fields
                StatsStoredFieldVisitor ssfv = new StatsStoredFieldVisitor();
                LeafReader reader = FilterLeafReader.unwrap(context.reader());
                for (int i = 0; i < reader.maxDoc(); i++) {
                    reader.document(i, ssfv);
                }
                for (FieldInfo field : reader.getFieldInfos()) {
                    if (!FIELD_STATS.containsKey(field.name))
                        FIELD_STATS.put(field.name, new ArrayList<>());
                    long docValues = ssfv.STATS.getOrDefault(field.name, 0L);
                    Terms terms = reader.terms(field.name);
                    if (terms instanceof FieldReader) {
                        Stats fieldStats = ((FieldReader) terms).getStats();
                        FIELD_STATS.get(field.name).add(new StatsHolder(fieldStats, docValues));
                        LOG.trace("{}: {}", field.name, fieldStats);
                    } else {
                        FIELD_STATS.get(field.name).add(new StatsHolder(null, docValues));
                    }
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
        dm.INDEX_GROUPS.forEach((s, indices) -> {
            for (ElasticsearchStateDecoder.Index indexItem : indices) {
                String fullIndexPath = String.format("%s\\%s\\0\\index", indexHome, indexItem.directoryName);
                loadIndexStats(fullIndexPath);
            }
            LOG.info("Processing Index Group: {}", s);
            FIELD_STATS.forEach((key, value) -> {
                long totalTermCount;

                long indexNumBytes;
                long totalTermBytes;
                long totalBlockSuffixBytes;
                long totalUncompressedBlockSuffixBytes;
                long totalBlockStatsBytes;
                long totalBlockOtherBytes;

                long totalStoredFieldBytes;

                totalTermCount = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalTermCount).sum();

                indexNumBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.indexNumBytes).sum();
                totalTermBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalTermBytes).sum();
                totalBlockSuffixBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalBlockSuffixBytes).sum();
                totalUncompressedBlockSuffixBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalUncompressedBlockSuffixBytes).sum();
                totalBlockStatsBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalBlockStatsBytes).sum();
                totalBlockOtherBytes = value.stream().mapToLong(statsHolder -> statsHolder.stats == null ? 0L : statsHolder.stats.totalBlockOtherBytes).sum();
                totalStoredFieldBytes = value.stream().mapToLong(statsHolder -> statsHolder.storedFieldBytes).sum();


                long fieldTotal = indexNumBytes + totalTermBytes + totalBlockSuffixBytes + totalUncompressedBlockSuffixBytes + totalBlockStatsBytes + totalBlockOtherBytes + totalStoredFieldBytes;
                CALCULATED_GROUP_SIZE += fieldTotal;

                LOG.info("  -> {}: Total: {} indexNumBytes: {} TermBytes: {} BlockSuffixBytes: {} UncompressedBlockSuffixBytes: {} BlockStatsBytes: {} BlockOtherByte: {} StoredFieldBytes: {}",
                        String.format("%-35s", key), fieldTotal, indexNumBytes, totalTermBytes, totalBlockSuffixBytes, totalUncompressedBlockSuffixBytes, totalBlockStatsBytes, totalBlockOtherBytes, totalStoredFieldBytes);
            });
            LOG.info("Index Group: {}, Index: {} bytes, Total bytes (Uncompressed): {}", s, String.format("%,-2d", INDEX_GROUP_SIZE), String.format("%,-2d", CALCULATED_GROUP_SIZE));
            LOG.info("--------------------------------------------------");

            FIELD_STATS.clear();
            INDEX_GROUP_SIZE = 0L;
            CALCULATED_GROUP_SIZE = 0L;
        });
    }

    class StatsHolder {
        Stats stats;
        Long storedFieldBytes;

        public StatsHolder(Stats stats, Long storedFieldBytes) {
            this.stats = stats;
            this.storedFieldBytes = storedFieldBytes;
        }
    }

}
