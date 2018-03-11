package com.sproutsocial.elasticsearch.plugins.eunomia

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test


/**
 * Created by awhite on 4/15/16.
 */
class SandboxTests {
    private val runner = ElasticsearchClusterRunner()

    @Before
    @Throws(Exception::class)
    fun setUp() {
        runner.onBuild { index, settingsBuilder ->
            settingsBuilder.put("plugin.types", "com.sproutsocial.elasticsearch.plugins.eunomia.EunomiaPlugin")
            settingsBuilder.put("transport.service.type", "prioritizing-transport-service")
            settingsBuilder.put("cluster.routing.allocation.same_shard.host", false)
            settingsBuilder.put("http.cors.enabled", true)
            settingsBuilder.put("http.cors.allow-origin", "*")
            settingsBuilder.putArray("discovery.zen.ping.unicast.hosts", "localhost:9301-9305")
            settingsBuilder.put("cluster.routing.allocation.disk.watermark.low", "95%")
            settingsBuilder.put("cluster.routing.allocation.disk.watermark.high", "99%")
        }.build(ElasticsearchClusterRunner.newConfigs().numOfNode(5))
        runner.ensureGreen()
    }

    @After
    fun tearDown() {
        runner.close()
        runner.clean()
    }

    private val myTypeMapping = """
        {
            "my-type" : {
                "properties" : {
                    "key" : { "type" : "string", "index" : "not_analyzed" },
                    "value" : { "type" : "string", "index" : "not_analyzed" }
                }
            }
        }
        """

    @Test
    @Throws(Exception::class)
    fun testTempestShardsAllocator() {
        val settings = Settings.settingsBuilder()
                .put("index.number_of_replicas", "1")
                .put("index.number_of_shards", "3")
                .build()
        runner.createIndex("my-index", settings)
        runner.createMapping("my-index", "my-type", myTypeMapping)

        if (!runner.indexExists("my-index")) { fail() }
        runner.ensureGreen()

        for (i in 1..100) {
            runner.insert("my-index", "my-type", i.toString(), createDocument(i)).run {
                assertTrue(this.isCreated)
            }
        }

        runner.client()
                .prepareSearch("my-index")
                .setTypes("my-type")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(100)
                .putHeader("eunomia-priority-level", "very high")
                .setPreference("_primary_first")
                .execute()
                .actionGet()
                .hits
                .hits
                .forEach { println(it.source) }
    }

    private fun createDocument(id: Int): String = """
        {
            "key": "key-$id",
            "value": "value-$id"
        }
        """



}