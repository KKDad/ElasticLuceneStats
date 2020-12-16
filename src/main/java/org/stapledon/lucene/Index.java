package org.stapledon.lucene;

class Index {
    private final String indexHome;
    private final String indexName;
    private final String directoryName;

    // Disk Sizes
    public Long indexByteSize;
    public Long transLogByteSize;

    // Number of Documents in the Index
    long numDocs;
    long numDeleted;

    public String getIndexDirectoryName() {
        return String.format("%s\\%s\\0\\index", indexHome, directoryName);
    }
    public String getTransLogDirectoryName() {
        return String.format("%s\\%s\\0\\translog", indexHome, directoryName);
    }

    public Index(String indexHome, String indexName, String directoryName) {
        this.indexHome = indexHome.replace("_state", "indices");
        this.indexName = indexName;
        this.directoryName = directoryName;

    }
    public void updateDocs(long numDocs, long numDeleted) {
        this.numDocs += numDocs;
        this.numDeleted += numDeleted;
    }


    @Override
    public String toString() {
        return String.format("Index{indexName='%s', directoryName='%s'}", indexName, directoryName);
    }
}