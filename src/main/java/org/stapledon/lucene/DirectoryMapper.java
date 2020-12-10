package org.stapledon.lucene;

import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.xcontent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DirectoryMapper {
    private static final Logger LOG = LoggerFactory.getLogger(DirectoryMapper.class);

    public static final Map<String, String> DIRECTORY_MAPPINGS = new HashMap<>();
    public static final Map<String, String> INDEX_MAPPINGS = new HashMap<>();

    DirectoryMapper(String directory) {
        this.process(directory);
    }

    protected void process(String directory) {
        try {
            Directory indexDirectory = FSDirectory.open(Paths.get(directory));
            try (IndexReader indexReader = DirectoryReader.open(indexDirectory)) {
                for (LeafReaderContext context : indexReader.leaves()) {
                    LeafReader reader = context.reader();
                    Bits liveDocs = reader.getLiveDocs();
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        if (liveDocs != null && !liveDocs.get(i))
                            continue;
                        extractDocument(reader, i);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    private void extractDocument(LeafReader reader, int i) throws IOException {
        IndexableField data = reader.document(i).getField("data");
        Map<String, Object> index = decodeXContent(data.binaryValue().bytes);
        Map<String, Object> indexMetadata = (Map<String, Object>) index.get(index.keySet().toArray()[0]);
        Map<String, Object> settings = (Map<String, Object>) indexMetadata.get("settings");
        if (settings == null)
            return;
        String indexDirectoryName = settings.get("index.uuid").toString();
        String indexName = settings.get("index.provided_name").toString();
        LOG.info("{} -> {}", indexDirectoryName, indexName);

        DIRECTORY_MAPPINGS.put(indexDirectoryName, indexName);
        INDEX_MAPPINGS.put(indexName, indexDirectoryName);
    }

    private Map<String, Object> decodeXContent(byte[] content) {
        try {
            XContent xContent = XContentFactory.xContent(XContentType.SMILE);
            try (XContentParser parser = xContent.createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, content)) {
                return parser.map();
            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
        return new HashMap<>();
    }

}
