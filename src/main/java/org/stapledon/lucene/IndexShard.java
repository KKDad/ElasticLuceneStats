package org.stapledon.lucene;

class IndexShard {
    private final String indexHome;
    private final String indexName;
    private final String directoryName;

    // Disk Sizes
    public long indexByteSize;
    public long transLogByteSize;

    // Number of Documents in the Index
    long numDocs;
    long numDeleted;

    public String getIndexDirectoryName() {
        return String.format("%s\\%s\\0\\index", indexHome, directoryName);
    }
    public String getTransLogDirectoryName() {
        return String.format("%s\\%s\\0\\translog", indexHome, directoryName);
    }

    public IndexShard(String indexHome, String indexName, String directoryName) {
        this.indexHome = indexHome.replace("_state", "indices");
        this.indexName = indexName;
        this.directoryName = directoryName;

    }
    public void updateDocs(long numDocs, long numDeleted) {
        this.numDocs += numDocs;
        this.numDeleted += numDeleted;
    }

    public void updateDiskUsage(long indexByteSize, long transLogByteSize) {
        this.indexByteSize +=indexByteSize;
        this.transLogByteSize += transLogByteSize;
    }

    @Override
    public String toString() {
        return String.format("  %s, %,d docs, %,d deleted docs, %,d bytes, %,2.2f bytes/doc  dir: %s", indexName, numDocs, numDeleted, indexByteSize, (float)indexByteSize /numDocs, directoryName);
    }
}