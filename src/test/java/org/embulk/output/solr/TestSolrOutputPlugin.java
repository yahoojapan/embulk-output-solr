/**
 * The MIT License (MIT)
 *
 * Copyright (C) 2016 Yahoo Japan Corporation. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package org.embulk.output.solr;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.solr.SolrOutputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestSolrOutputPlugin {

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private SolrOutputPlugin plugin;

    private static String SOLR_HOST;
    private static int SOLR_PORT;
    private static String SOLR_COLLECTION;
    private static int SOLR_BULK_SIZE;

    private static String PATH_PREFIX;

    @BeforeClass
    public static void initializeConstant() {
        SOLR_HOST = System.getenv("SOLR_HOST") != null ? System.getenv("SOLR_HOST") : "localhost";
        SOLR_PORT = System.getenv("SOLR_PORT") != null ? Integer.valueOf(System.getenv("SOLR_PORT")) : 8983;
        SOLR_COLLECTION = System.getenv("SOLR_COLLECTION") != null ? System.getenv("SOLR_COLLECTION") : "mytest";
        SOLR_BULK_SIZE = System.getenv("SOLR_BULK_SIZE") != null ? Integer.valueOf(System.getenv("SOLR_BULK_SIZE"))
                : 1000;

        PATH_PREFIX = SolrOutputPlugin.class.getClassLoader().getResource("sample_01.csv").getPath();
    }

    @Before
    public void createResources() {
        plugin = new SolrOutputPlugin();
    }

    @Test
    public void testDefaultValue() {
        ConfigSource config = config();
        SolrOutputPlugin.PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals(SOLR_PORT, task.getPort());
        assertEquals(SOLR_BULK_SIZE, task.getBulkSize());
    }

    @Test
    public void testTransaction() {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig()
                .toSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource) {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
    }

    @Test
    public void testResume() {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig()
                .toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control() {
            @Override
            public List<TaskReport> run(TaskSource taskSource) {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup() {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig()
                .toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), schema, 0, Arrays.asList(Exec.newTaskReport()));
        // no error happens
    }

      // you cannnot execute following test case without Solr runnning on localhost.
//    @Test
//    public void testOutputByOpen() throws SolrServerException, IOException, NoSuchMethodException, SecurityException,
//            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
//        ConfigSource config = config();
//        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig()
//                .toSchema();
//        PluginTask task = config.loadConfig(PluginTask.class);
//        plugin.transaction(config, schema, 0, new OutputPlugin.Control() {
//            @Override
//            public List<TaskReport> run(TaskSource taskSource) {
//                return Lists.newArrayList(Exec.newTaskReport());
//            }
//        });
//        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);
//
//        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, "1", 32864L,
//                Timestamp.ofEpochSecond(1422386629), Timestamp.ofEpochSecond(1422316800), true, 123.45, "embulk");
//        assertEquals(1, pages.size());
//        for (Page page : pages) {
//            output.add(page);
//        }
//
//        output.finish();
//        output.commit();
//
//        Method createClient = SolrOutputPlugin.class.getDeclaredMethod("createSolrClient", PluginTask.class);
//        createClient.setAccessible(true);
//        try (SolrClient client = (SolrClient) createClient.invoke(plugin, task)) {
//            SolrQuery query = new SolrQuery();
//            query.set("q", "id:1");
//            QueryResponse response = client.query(query);
//
//            assertEquals(1, response.getResults().size());
//            if (response.getResults().size() > 0) {
//
//                SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//                dateFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
//
//                SolrDocument map = response.getResults().get(0);
//                assertEquals("1", map.get("id"));
//                assertEquals(32864L, map.get("account_l"));
//                assertEquals("2015-01-27T19:23:49.000Z", dateFormatter.format(map.get("time_dt")));
//                assertEquals("2015-01-27T00:00:00.000Z", dateFormatter.format(map.get("purchase_dt")));
//                assertEquals(true, map.get("flg_b"));
//                assertEquals(123.45, map.get("score_d"));
//                assertEquals("embulk", map.get("comment_s"));
//            }
//        }
//    }

    private ConfigSource config() {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "solr")
                .set("host", SOLR_HOST)
                .set("port", SOLR_PORT)
                .set("collection", SOLR_COLLECTION)
                .set("bulkSize", SOLR_BULK_SIZE)
                .set("idColumnName", "id");
    }
    
    private ImmutableMap<String, Object> inputConfig() {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig) {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig() {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "string"));
        builder.add(ImmutableMap.of("name", "account_l", "type", "long"));
        builder.add(ImmutableMap.of("name", "time_dt", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase_dt", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "flg_b", "type", "boolean"));
        builder.add(ImmutableMap.of("name", "score_d", "type", "double"));
        builder.add(ImmutableMap.of("name", "comment_s", "type", "string"));
        return builder.build();
    }
}
