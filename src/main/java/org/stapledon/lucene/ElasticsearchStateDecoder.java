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
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Read and parse the "_state" index from an Elasticsearch node.
 *
 * This index tracks all of the settings for all elasticsearch indexes in a Lucene index with a single stored field
 * called "data". This data field contains a SMILE-encoded map. Example:
 *
 * {reporting_tags_adm=
 *        {settings=
 *        {index.analysis.filter.typeahead-ngrams.side=front,
 * 		 index.creation_date=1603912611994,
 * 		 index.uuid=IJdq3RdBSSKR2SRitZmBcw,
 * 		 index.version.created=7060299,
 * 		 index.analysis.analyzer.typeahead-search-analyzer.tokenizer=whitespace,
 * 		 index.analysis.filter.typeahead-ngrams.min_gram=1,
 * 		 index.analysis.analyzer.typeahead-analyzer.filter=[typeahead-ngrams,lowercase],
 * 		 index.analysis.filter.typeahead-ngrams.type=edge_ngram,
 * 		 index.provided_name=reporting_tags_adm,
 * 		 index.number_of_replicas=0,
 * 		 index.analysis.filter.typeahead-ngrams.max_gram=140,
 * 		 index.analysis.analyzer.typeahead-analyzer.type=custom,
 * 		 index.analysis.analyzer.typeahead-search-analyzer.filter=[lowercase],
 * 		 index.analysis.analyzer.typeahead-search-analyzer.type=custom,
 * 		 index.analysis.analyzer.typeahead-analyzer.tokenizer=whitespace,
 * 		 index.number_of_shards=1
 *         },
 * 	 mappings=[[B@29fbb122],
 * 	 aliases={},
 * 	 settings_version=1,
 * 	 state=open,
 * 	 in_sync_allocations={0=[siIOtwSxTIaakTP4k9N6Ng]},
 * 	 rollover_info={},
 * 	 version=3,
 * 	 mapping_version=1,
 * 	 aliases_version=1,
 * 	 routing_num_shards=1024,
 * 	 primary_terms=[1]
 *    }
 *  }
 *
 */
public class ElasticsearchStateDecoder {
    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchStateDecoder.class);

    private final Map<String, String> indexMappings = new TreeMap<>();

    public final Map<String, List<Index>> INDEX_GROUPS = new TreeMap<>();

    // Parse index names are in the format of <Index>_<TID>[_-]<Postfix> into logical groupings
    private static final String INDEX_NAME_PATTERN = "([a-zA-Z_]+){1,3}_(\\d){1,3}[-_]?(.*)";
    private final Pattern pattern = Pattern.compile(INDEX_NAME_PATTERN);

    ElasticsearchStateDecoder(String directory) {
        LOG.warn("Reading {}", directory);
        this.decode(directory);
        this.generateGroupings();
    }

    private void generateGroupings() {
        indexMappings.forEach((indexName, value) -> {
            Matcher m = pattern.matcher(indexName);
            String indexNameShort = indexName;
            if (m.matches())
                indexNameShort = m.group(1);
            INDEX_GROUPS.putIfAbsent(indexNameShort, new ArrayList<>());
            INDEX_GROUPS.get(indexNameShort).add(new Index(indexName, value));
        });

        INDEX_GROUPS.forEach((key, value) -> {
            LOG.info("Index {}", key);
            for (Index index : value) {
                LOG.info("  -> {}", index);
            }
        });
    }

    protected void decode(String directory) {
        try {
            Directory indexDirectory = FSDirectory.open(Paths.get(directory));
            try (IndexReader indexReader = DirectoryReader.open(indexDirectory)) {
                for (LeafReaderContext context : indexReader.leaves()) {
                    LeafReader reader = context.reader();
                    Bits liveDocs = reader.getLiveDocs();
                    for (int i = 0; i < reader.maxDoc(); i++) {
                        if (liveDocs != null && !liveDocs.get(i))
                            continue;
                        processIndexEntry(reader, i);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    private void processIndexEntry(LeafReader reader, int i) throws IOException {
        IndexableField data = reader.document(i).getField("data");
        Map<String, Object> index = decodeXContent(data.binaryValue().bytes);
        LOG.trace("{}", index);

        Map<String, Object> indexMetadata = (Map<String, Object>) index.get(index.keySet().toArray()[0]);
        Map<String, Object> settings = (Map<String, Object>) indexMetadata.get("settings");
        if (settings == null)
            return;
        String indexDirectoryName = settings.get("index.uuid").toString();
        String indexName = settings.get("index.provided_name").toString();

        indexMappings.put(indexName, indexDirectoryName);
    }

    /**
     * Decode the SMILE-encoded map
     *
     * @param content - Byte content from a stored field
     * @return Java Map
     */
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

    class Index {
        public final String indexName;
        public final String directoryName;

        public Index(String indexName, String directoryName) {
            this.indexName = indexName;
            this.directoryName = directoryName;

        }

        @Override
        public String toString() {
            return String.format("Index{indexName='%s', directoryName='%s'}", indexName, directoryName);
        }
    }

}
