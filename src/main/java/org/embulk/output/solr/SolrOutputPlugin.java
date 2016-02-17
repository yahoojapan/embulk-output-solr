package org.embulk.output.solr;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import com.google.common.base.Throwables;
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
        throw new UnsupportedOperationException("solr output plugin does not support resuming");
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
        SolrClient solr = new HttpSolrClient(
                "http://" + task.getHost() + ":" + task.getPort() + "/solr/" + task.getCollection());
        return solr;
    }

    public static class SolrPageOutput implements TransactionalPageOutput {

        private Logger logger;
        private SolrClient client;
        private final PageReader pageReader;
        private final Schema schema;
        private PluginTask task;
        private final int bulkSize;
        
        List<SolrInputDocument> documentList = new LinkedList<SolrInputDocument>();

        public SolrPageOutput(SolrClient client, Schema schema, PluginTask task) {
            this.logger = Exec.getLogger(getClass());
            this.client = client;
            this.pageReader = new PageReader(schema);
            this.schema = schema;
            this.task = task;
            this.bulkSize = task.getBulkSize();
        }

        @Override
        public void add(Page page) {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                final SolrInputDocument doc = new SolrInputDocument();
                
                schema.visitColumns(new ColumnVisitor() {

                    @Override
                    public void booleanColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            boolean value = pageReader.getBoolean(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void longColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            long value = pageReader.getLong(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            double value = pageReader.getDouble(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void stringColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            String value = pageReader.getString(column);
                            doc.addField(column.getName(), value);
                        }
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            Timestamp value = pageReader.getTimestamp(column);
                            Date dateValue = new Date(value.getEpochSecond());
                            doc.addField(column.getName(), dateValue);
                        }
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            //do nothing.
                        } else {
                            Value value = pageReader.getJson(column);
                            doc.addField(column.getName(), value.toString()); // send json as string.
                        }
                    }
                });
                
                System.out.println(doc);
                
                documentList.add(doc);
                
                if (documentList.size() >= bulkSize) {
                    try {
                        client.add(documentList);
                        client.commit();
                        documentList.clear();
                    } catch (SolrServerException | IOException e) {
                        Throwables.propagate(e); //  TODO error handling
                    }
                }
            }
        }

        @Override
        public void finish() {
            // send rest of all documents.
            if (!documentList.isEmpty()) {
                try {
                    client.add(documentList);
                    client.commit();
                    documentList.clear();
                } catch (SolrServerException | IOException e) {
                    Throwables.propagate(e); //  TODO error handling
                }
            }
        }

        @Override
        public void close() {
            try {
                client.close();
                client = null;
            } catch (IOException e) {
                Throwables.propagate(e); //  TODO error handling
            }
        }

        @Override
        public void abort() {
        }

        @Override
        public TaskReport commit() {
            TaskReport report = Exec.newTaskReport();
            return report;
        }
    }
}
