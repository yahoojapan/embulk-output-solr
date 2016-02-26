package org.embulk.output.solr;

import java.io.IOException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import com.google.common.base.Optional;
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
import org.embulk.spi.type.BooleanType;
import org.embulk.spi.type.DoubleType;
import org.embulk.spi.type.LongType;
import org.embulk.spi.type.StringType;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.type.Type;
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

        @Config("idColumnName")
        public String getIdColumnName();
        
        @Config("multiValuedField")
        @ConfigDefault("null")
        public Optional<List<String>> getMultiValuedField();
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
        // solr.setConnectionTimeout(10000); // 10 seconds for timeout.
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
        
	private List<SolrInputDocument> documentList = new LinkedList<SolrInputDocument>();
	private SolrInputDocument lastDoc = null;

        @Override
        public void add(Page page) {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                
                final boolean mergeDoc = isSameIDWithLastDoc(pageReader, lastDoc, task.getMultiValuedField().orNull());
                final SolrInputDocument doc = new SolrInputDocument();
                final List<String> multValuedField = task.getMultiValuedField().orNull();
                
                schema.visitColumns(new ColumnVisitor() {

                    @Override
                    public void booleanColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            boolean value = pageReader.getBoolean(column);
                            addFieldValue(mergeDoc, doc, multValuedField, column, value);
                        }
                    }

                    @Override
                    public void longColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            long value = pageReader.getLong(column);
                            addFieldValue(mergeDoc, doc, multValuedField, column, value);
                        }
                    }

                    @Override
                    public void doubleColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            double value = pageReader.getDouble(column);
                            addFieldValue(mergeDoc, doc, multValuedField, column, value);
                        }
                    }

                    @Override
                    public void stringColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            String value = pageReader.getString(column);
                            if (value != null && !value.equals("")){
                                addFieldValue(mergeDoc, doc, multValuedField, column, value);
                            }
                        }
                    }

                    @Override
                    public void timestampColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            Timestamp value = pageReader.getTimestamp(column);
                            if (!value.equals(""))  {
                                Date dateValue = new Date(value.getEpochSecond() * 1000);
                                addFieldValue(mergeDoc, doc, multValuedField, column, dateValue);
                            }
                        }
                    }

                    @Override
                    public void jsonColumn(Column column) {
                        if (pageReader.isNull(column)) {
                            // do nothing.
                        } else {
                            Value value = pageReader.getJson(column);
                            // send json as a string.
                            if (!value.equals("")) {
                                addFieldValue(mergeDoc, doc, multValuedField, column, value.toString());
                            }
                        }
                    }
                    

                    private void addFieldValue(final boolean mergeDoc, final SolrInputDocument doc,
                            final List<String> multValuedField, Column column, Object value) {
                        if (mergeDoc) {
                            String n = column.getName();
                            for (String f : multValuedField) {
                                if (n.equals(f)) {
                                    lastDoc.addField(column.getName(), value);
                                    return;
                                }
                            }
                        } else {
                            doc.addField(column.getName(), value);
                        }
                    }
                });
                
                if(!mergeDoc) {
                    documentList.add(doc);
                    lastDoc = doc;
                }
                
                if (documentList.size() >= bulkSize) {
                    sendDocumentToSolr(documentList);
                }
            }
            
            if (documentList.size() >= bulkSize) {
               sendDocumentToSolr(documentList);
            }
        }

        private boolean isSameIDWithLastDoc(PageReader pageReader, SolrInputDocument lastDoc, List<String> multiValuedFieldList) {
            if (multiValuedFieldList == null || multiValuedFieldList.size()==0){
                return false;
            }
            boolean mergeDoc = false;
            List<Column> columnList = pageReader.getSchema().getColumns();
            for (Column c : columnList) {
                if (c.getName().equals(task.getIdColumnName())) {
                    Type type = c.getType();
                    if (type instanceof BooleanType) {
                        boolean value = pageReader.getBoolean(c);
                        if (lastDoc != null) {
                            boolean b = (boolean) lastDoc.get(task.getIdColumnName()).getValue();
                            if (value==b) {
                                mergeDoc = true;
                                break;
                            }
                        }
                    } else if (type instanceof LongType) {
                        long value = pageReader.getLong(c);
                        if (lastDoc != null) {
                            long b = (long) lastDoc.get(task.getIdColumnName()).getValue();
                            if (value==b) {
                                mergeDoc = true;
                                break;
                            }
                        }
                    } else if (type instanceof DoubleType) {
                        double value = pageReader.getDouble(c);
                        if (lastDoc != null) {
                            double b = (double) lastDoc.get(task.getIdColumnName()).getValue();
                            if (value==b) {
                                mergeDoc = true;
                                break;
                            }
                        }
                    } else if (type instanceof StringType) {
                        String value = pageReader.getString(c);
                        if (lastDoc != null) {
                            String b = (String) lastDoc.get(task.getIdColumnName()).getValue();
                            if (value.equals(b)) {
                                mergeDoc = true;
                                break;
                            }
                        }
                    } else if (type instanceof TimestampType) {
                        Timestamp value = pageReader.getTimestamp(c);
                        Date dateValue = new Date(value.getEpochSecond() * 1000);
                        if (lastDoc != null) {
                            Date b = (Date) lastDoc.get(task.getIdColumnName()).getValue();
                            if (dateValue.compareTo(b)==0) {
                                mergeDoc = true;
                                break;
                            }
                        }
                    } else {
                        throw new RuntimeException("Could not find column definition for name : " + task.getIdColumnName());
                    }
                }
            }
            return mergeDoc;
        }

        private void sendDocumentToSolr(List<SolrInputDocument> documentList) {
            int retrycount = 0;
            while(true) {
                try {
                    logger.debug("start sending document to solr.");
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
                // if you get Bad request from server, then the thread will be die...
                //} catch (RemoteSolrException e) {
                //    logger.error("bad request ? : ", e);
                //}
            }
        }

        @Override
        public void finish() {
            try {
                sendDocumentToSolr(documentList);
                client.commit();
                logger.info("Done sending document to Solr in the thread! Total Count : " + totalCount);
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
            TaskReport report = Exec.newTaskReport();
            return report;
        }
    }
}
