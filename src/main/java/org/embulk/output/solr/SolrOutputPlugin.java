package org.embulk.output.solr;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import com.google.inject.Inject;

import org.embulk.config.TaskReport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.msgpack.value.Value;
import org.slf4j.Logger;

public class SolrOutputPlugin implements OutputPlugin {
    public interface PluginTask extends Task {
        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("8983")
        public int getPort();

        @Config("collection")
        public String getCollection();

        @Config("maxRetry")
        @ConfigDefault("5")
        public int getMaxRetry();
        
        @Config("bulkSize")
        @ConfigDefault("1000")
        public int getBulkSize();
    }

    private final Logger logger;
    
    @Inject
    public SolrOutputPlugin() {
        logger = Exec.getLogger(getClass());
    }

    @Override
    public ConfigDiff transaction(ConfigSource config, Schema schema, int taskCount, OutputPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource, Schema schema, int taskCount, OutputPlugin.Control control) {
        // TODO
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, Schema schema, int taskCount, List<TaskReport> successTaskReports) {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        SolrClient client = createSolrClient(task);
        TransactionalPageOutput pageOutput = new SolrPageOutput(client, schema, task);
        return pageOutput;
    }

    /**
     * create Solr Client. <br>
     * this method has not been supporting SolrCloud client using zookeeper
     * Host, yet.
     * 
     * @param task
     * @return SolrClient instance.
     */
    private SolrClient createSolrClient(PluginTask task) {
        HttpSolrClient solr = new HttpSolrClient(
                "http://" + task.getHost() + ":" + task.getPort() + "/solr/" + task.getCollection());
        solr.setConnectionTimeout(10000); // 10 seconds for timeout.
        return solr;
    }

    public static class SolrPageOutput implements TransactionalPageOutput {

        private Logger logger;
        private SolrClient client;
        private final PageReader pageReader;
        private final Schema schema;
        private PluginTask task;
        private final int bulkSize;

        private final int maxRetry;

        public SolrPageOutput(SolrClient client, Schema schema, PluginTask task) {
            this.logger = Exec.getLogger(getClass());
            this.client = client;
            this.pageReader = new PageReader(schema);
            this.schema = schema;
            this.task = task;
            this.bulkSize = task.getBulkSize();
            this.maxRetry = task.getMaxRetry();
        }
        
        int totalCount = 0;
        
        private static List<SolrInputDocument> documentList = java.util.Collections.synchronizedList(new java.util.ArrayList<SolrInputDocument>());
        
        @Override
        public void add(Page page) {

            // List<SolrInputDocument> documentList = new LinkedList<SolrInputDocument>();
            
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final SolrInputDocument doc = new SolrInputDocument();
                totalCount ++;

                schema.visitColumns(new ColumnVisitor() {

                    @Override
                    public void booleanColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            boolean value = pageReader.getBoolean(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void longColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            long value = pageReader.getLong(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            double value = pageReader.getDouble(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void stringColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            String value = pageReader.getString(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            Timestamp value = pageReader.getTimestamp(column);
                            Date dateValue = new Date(value.getEpochSecond() * 1000);
                            doc.addField(column.getName(), dateValue);
                        }
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            Value value = pageReader.getJson(column);
                            // send json as a string.
                            doc.addField(column.getName(), value.toString());
                        }
                    }
                });

                documentList.add(doc);
                if (documentList.size() >= bulkSize) {
                    sendDocumentToSolr(documentList);
                }
            }
            
            sendDocumentToSolr(documentList);
        }

        private void sendDocumentToSolr(List<SolrInputDocument> documentList) {
            int retrycount = 0;
            while(true) {
                try {
                    client.add(documentList);
                    logger.debug("successfully load a bunch of documents to solr. batch count : " + documentList.size() + " current total count : " + totalCount);
                    documentList.clear(); // when successfully add and commit, clear list.
                    break;
                } catch (SolrServerException | IOException e) {
                    if (retrycount < maxRetry) {
                        retrycount++;
                        logger.debug("RETRYing : " + retrycount);
                        continue;
                    } else {
                        logger.error("failed to send document to solr. ", e);
                        documentList.clear();
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        @Override
        public void finish() {
            try {
                sendDocumentToSolr(documentList);
                client.commit();
                logger.info("Done sending document to Solr ! total count : " + totalCount);
            } catch (SolrServerException | IOException e) {
                logger.info("failed to commit document. ", e);
            }
        }

        @Override
        public void close() {
            pageReader.close();
            try {
                client.close();
                client = null;
            } catch (IOException e) {
                // do nothing.
            }
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit() {
            
            logger.info("commit !!!!!!!!!!!!!!!!!!!!!!!!");
            
            TaskReport report = Exec.newTaskReport();
            return report;
        }
    }
}
