package org.elasticsearch.plugin.sortbydoc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.node.internal.InternalSettingsPreparer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.query.sortbydoc.SortByDocQueryBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
public class SortByDocTest {
    private ObjectMapper objectMapper = new ObjectMapper();
    final String index = "test_index";
    private Client client;
    private Node node;


    @Before
    public void prepare() throws NodeValidationException {
        node = newNode();
        client = node.client();
    }

    @After
    public void after() throws IOException {
        client.close();
        node.close();
    }


    @Test
    public void testIndexFetch() throws Exception {
        indexObject(new E("1", "A"));
        indexObject(new E("2", "A"));
        indexObject(new E("3", "C"));
        indexObject(new L("l1", Arrays.asList(new LE("1", 1), new LE("2", 3), new LE("3", 2))));
        indexObject(new L("l2", Collections.singletonList(new LE("1", 3))));
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        final SearchResponse test = client.prepareSearch(index).setTypes(E.TYPE).setQuery(QueryBuilders.matchAllQuery()).execute().actionGet();
        Assert.assertEquals(3, test.getHits().getTotalHits());


        // Sort by doc on score ASC
        SortByDocQueryBuilder builder = new SortByDocQueryBuilder()
                .query(QueryBuilders.matchAllQuery())
                .lookupIndex(index)
                .lookupType(L.TYPE)
                .lookupId("l1")
                .idField("id")
                .sortOrder(SortOrder.ASC)
                .rootPath("elements")
                .scoreField("score");

        final SearchResponse test2 = client.prepareSearch(index).setTypes(E.TYPE).setQuery(builder).execute().actionGet();
        Assert.assertEquals(3, test2.getHits().getTotalHits());

        Assert.assertEquals("1", test2.getHits().getHits()[0].getSource().get("id"));
        Assert.assertEquals("3", test2.getHits().getHits()[1].getSource().get("id"));

        // Sort by doc on score ASC
        builder = new SortByDocQueryBuilder()
                .query(QueryBuilders.matchAllQuery())
                .lookupIndex(index)
                .lookupType(L.TYPE)
                .lookupId("l1")
                .idField("id")
                .sortOrder(SortOrder.DESC)
                .rootPath("elements")
                .scoreField("score");

        final SearchResponse test3 = client.prepareSearch(index).setTypes(E.TYPE).setQuery(builder).execute().actionGet();
        Assert.assertEquals(3, test3.getHits().getTotalHits());

        Assert.assertEquals("2", test3.getHits().getHits()[0].getSource().get("id"));
        Assert.assertEquals("3", test3.getHits().getHits()[1].getSource().get("id"));


        // Sort by doc on score ASC but with a list with only one score
        builder = new SortByDocQueryBuilder()
                .query(QueryBuilders.matchAllQuery())
                .lookupIndex(index)
                .lookupType(L.TYPE)
                .lookupId("l2")
                .idField("id")
                .sortOrder(SortOrder.DESC)
                .rootPath("elements")
                .scoreField("score");
        final SearchResponse test4 = client.prepareSearch(index).setTypes(E.TYPE).setQuery(builder).execute().actionGet();
        Assert.assertEquals(1, test4.getHits().getTotalHits());

        // Sort by doc on score ASC with score restriction
        builder = new SortByDocQueryBuilder()
                .query(QueryBuilders.matchAllQuery())
                .lookupIndex(index)
                .lookupType(L.TYPE)
                .lookupId("l1")
                .idField("id")
                .sortOrder(SortOrder.ASC)
                .rootPath("elements")
                .scoreField("score")
                .minScore(2.0f)
                .maxScore(3.0f);

        final SearchResponse test5 = client.prepareSearch(index).setTypes(E.TYPE).setQuery(builder).execute().actionGet();
        Assert.assertEquals(2, test5.getHits().getTotalHits());

        Assert.assertEquals("3", test5.getHits().getHits()[0].getSource().get("id"));
        Assert.assertEquals("2", test5.getHits().getHits()[1].getSource().get("id"));

    }


    @Test
    public void testIndexFetchWithSubquery() throws Exception {
        indexObject(new E("1", "A"));
        indexObject(new E("2", "A"));
        indexObject(new E("3", "C"));
        indexObject(new L("l1", Arrays.asList(new LE("1", 1), new LE("2", 3), new LE("3", 2))));
        client.admin().indices().prepareRefresh(index).execute().actionGet();

        // lookup is useless here, it's just to have a more realistic case (with mandatory query rewrite)
        BoolQueryBuilder onlyTypeA = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("type", "a"))
                .filter(QueryBuilders.termsLookupQuery("_id", new TermsLookup(index, L.TYPE, "l1", "elements.id"))); //
        final SearchResponse test = client.prepareSearch(index).setTypes(E.TYPE).setQuery(onlyTypeA).execute().actionGet();
        Assert.assertEquals(2, test.getHits().getTotalHits());


        // Sort by doc on score ASC
        SortByDocQueryBuilder builder = new SortByDocQueryBuilder()
                .query(onlyTypeA)
                .lookupIndex(index)
                .lookupType(L.TYPE)
                .lookupId("l1")
                .idField("id")
                .sortOrder(SortOrder.ASC)
                .rootPath("elements")
                .scoreField("score");

        final SearchResponse test2 = client.prepareSearch(index).setTypes(E.TYPE).setQuery(builder).execute().actionGet();
        Assert.assertEquals(2, test2.getHits().getTotalHits());

        Assert.assertEquals("1", test2.getHits().getHits()[0].getSource().get("id"));
        Assert.assertEquals("2", test2.getHits().getHits()[1].getSource().get("id"));

    }

    private void indexObject(E o) throws JsonProcessingException {
        String source = objectMapper.writeValueAsString(o);
        client.prepareIndex(index, o.getClass().getSimpleName(), o.id).setSource(source).execute().actionGet();
    }

    private void indexObject(L o) throws JsonProcessingException {
        String source = objectMapper.writeValueAsString(o);
        client.prepareIndex(index, o.getClass().getSimpleName(), o.id).setSource(source).execute().actionGet();
    }


    public static class E {
        public static final String TYPE = E.class.getSimpleName();
        public String id;
        public String type;

        public E(String id, String type) {
            this.id = id;
            this.type = type;
        }
    }


    public static class L {
        public static final String TYPE = L.class.getSimpleName();
        public String id;
        public List<LE> elements;

        public L(String id, List<LE> elements) {
            this.id = id;
            this.elements = elements;
        }
    }

    public static class LE {
        public String id;
        public double score;

        public LE(String id, double score) {
            this.id = id;
            this.score = score;
        }
    }

    private static void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                deleteDir(f);
            }
        }
        file.delete();
    }
    private static Node newNode() throws NodeValidationException {
        File index = new File("/var/tmp/data/");
        if (index.exists()) {
            deleteDir(index);
        }
        String name = "junit-sbd";
        Settings settings = Settings.builder()
                .put("http.enabled", false)
                .put("node.name", name)
                .put("path.home", "/var/tmp")
                .put("transport.type", "local")
                .build();
        // We need a cluster name in dev otherwise, the system connects automatically to another ES with same name
        Node node = new NodeWithPlugins(InternalSettingsPreparer.prepareEnvironment(settings, null),
                Collections.singletonList(SortByDocPlugin.class));
        node.start();
        return node;
    }


    private static class NodeWithPlugins extends Node {
        NodeWithPlugins(Environment environment, Collection<Class<? extends Plugin>> classpathPlugins) {
            super(environment, classpathPlugins);
        }
    }


}
