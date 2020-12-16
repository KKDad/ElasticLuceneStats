package org.stapledon.lucene;

import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.index.IndexOptions;

public class FieldStatsHolder {
    private final IndexOptions indexOptions;
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

    public FieldStatsHolder(String name, IndexOptions indexOptions) {
        this.name = name;
        this.indexOptions = indexOptions;
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
        return String.format("%-35s (%5.2f%%), %-30s   Stored=%,d, IndexBytes=%,d, TermBytes=%,d, BlockSuffixBytes=%,d, UncompressedBlockSuffixBytes=%,d, BlockStatsBytes=%,d, BlockOtherBytes=%,d, FieldTotal=%,d",
                name, percentage, indexOptions, storedFieldBytes, indexNumBytes, totalTermBytes, totalBlockSuffixBytes, totalUncompressedBlockSuffixBytes, totalBlockStatsBytes, totalBlockOtherBytes, getTotal());
    }
}
