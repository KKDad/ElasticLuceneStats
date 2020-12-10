package org.stapledon.lucene;

import org.apache.commons.cli.CommandLine;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;

public class LuceneStats {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneStats.class);

    public static void main(String[] args) {
        LuceneStats ls = new LuceneStats();
        CommandLine cmd = StartupUtils.parseOptions(args);
        ls.run(cmd.getOptionValue("indexDirectory"));
    }



    public void run(String directory) {
        try {
            Directory indexDirectory = FSDirectory.open(Paths.get(directory));
//            for (String item : indexDirectory.listAll()) {
//                LOG.info("-> {}", item);
//            }
            IndexReader indexReader = DirectoryReader.open(indexDirectory);
            LOG.info( "Number of Docs: {}", indexReader.numDocs());
//            for (LeafReaderContext context : indexReader.leaves()) {
//                LeafReader reader = FilterLeafReader.unwrap(context.reader());
//                Iterator<FieldInfo> iterator = reader.getFieldInfos().iterator();
//                while (iterator.hasNext()) {
//                    FieldInfo field = iterator.next();
//                    Terms terms = reader.terms(field.name);
//                    if (terms != null && terms instanceof FieldReader) {
//                        Stats fieldStats = ((FieldReader) terms).getStats();
//                        LOG.info("{}: {}", field.name, fieldStats);
//                    }
//                }
//            }
        } catch (IOException e) {
            LOG.error(e.getLocalizedMessage(), e);
        }
    }

    public void process(String directory) {
        DirectoryMapper dm = new DirectoryMapper(directory);
    }

}
