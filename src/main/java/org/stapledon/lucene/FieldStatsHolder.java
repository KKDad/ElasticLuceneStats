package org.stapledon.lucene;

import org.apache.lucene.codecs.blocktree.Stats;
import org.apache.lucene.index.IndexOptions;

import java.util.ArrayList;
import java.util.List;

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

    // Sum of Terms (Not unique)
    long totalTermCount;

    final List<String> sampleDocs = new ArrayList<>();



    // Total
    double percentage;
    Long getTotal() {
        return storedFieldBytes + indexNumBytes + totalTermBytes + totalBlockSuffixBytes + totalUncompressedBlockSuffixBytes + totalBlockStatsBytes + totalBlockOtherBytes;
    }

    public void calculate(long indexTotalBytes)
    {
        this.indexTotalBytes = indexTotalBytes;
        percentage = ((double)getTotal())  / indexTotalBytes * 100;
    }

    public FieldStatsHolder(String name, IndexOptions indexOptions) {
        this.name = name;
        this.indexOptions = indexOptions;
    }

    public void accumulateSample(String sampleDoc) {
        if (sampleDoc != null) {
            this.sampleDocs.add(sampleDoc);
        }
    }

    public void accumulateStats(Stats stats) {
        indexNumBytes += stats.indexNumBytes;
        totalTermBytes += stats.totalTermBytes;
        totalBlockSuffixBytes += stats.totalBlockSuffixBytes;
        totalUncompressedBlockSuffixBytes += stats.totalUncompressedBlockSuffixBytes;
        totalBlockStatsBytes += stats.totalBlockStatsBytes;
        totalBlockOtherBytes += stats.totalBlockOtherBytes;

        totalTermCount += stats.totalTermCount;
    }

    public void accumulateStoredFieldBytes(Long storedFieldBytes) {
        this.storedFieldBytes += storedFieldBytes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-35s (%5.2f%%), %-30s ",  name, percentage, indexOptions));
        if (getTotal() > 0 || storedFieldBytes> 0 || indexNumBytes > 0)
            sb.append(String.format("Field %,d bytes; Stored=%,d; IndexBytes=%,d; ", getTotal(), storedFieldBytes, indexNumBytes));
        if (totalTermCount > 0)
            sb.append(String.format("Terms=%d; %2.2f bytes/term", totalTermCount, (double) (totalTermBytes)/totalTermCount));
        if (totalTermBytes > 0 || totalBlockSuffixBytes > 0 || totalUncompressedBlockSuffixBytes > 0 || totalBlockStatsBytes > 0 || totalBlockOtherBytes > 0) {
            sb.append(
                    String.format("; TermBytes=%,d; BlockSuffixBytes=%,d; UncompressedBlockSuffixBytes=%,d; BlockStatsBytes=%,d; BlockOtherBytes=%,d",
                                  totalTermBytes, totalBlockSuffixBytes, totalUncompressedBlockSuffixBytes, totalBlockStatsBytes, totalBlockOtherBytes)
            );
        }

        return sb.toString();
    }
}
