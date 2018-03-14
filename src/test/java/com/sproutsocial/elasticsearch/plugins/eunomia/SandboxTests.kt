package com.sproutsocial.elasticsearch.plugins.eunomia

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.index.query.QueryBuilders
import org.junit.After
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import java.util.*


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

        runner.ensureGreen()

        (0..1000).forEach {
            Thread.sleep(100)
            doSomethingRandom(runner)
        }

        while (true) { Thread.sleep(1000) }
    }

    private fun doSomethingRandom(runner: ElasticsearchClusterRunner) {
        when (Random().nextInt(2)) {
            0 -> doSearch(runner)
            1 -> doBulkIndex(runner)
        }
    }

    private fun doBulkIndex(runner: ElasticsearchClusterRunner) {
        runner.client().prepareBulk()
                .apply { (0..100).forEach { this.add(buildRandomIndexRequest(runner)) } }
                .putHeader("eunomia-priority-level", pickRandom("very low", "low", "normal"))
                .putHeader("eunomia-priority-group", pickRandom("d"))
                .execute()
    }

    private fun buildRandomIndexRequest(runner: ElasticsearchClusterRunner): IndexRequest {
        val nextId = Random().nextInt(100)

        return runner.client()
                .prepareIndex("my-index", "my-type", nextId.toString())
                .setSource(createDocument(nextId, Random().nextInt()))
                .request()
    }

    private fun doSearch(runner: ElasticsearchClusterRunner) {
        runner.client()
                .prepareSearch("my-index")
                .setTypes("my-type")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(100)
                .putHeader("eunomia-priority-level", pickRandom("very high", "high", "normal"))
                .putHeader("eunomia-priority-group", pickRandom("a", "b", "c"))
                .execute()
    }

    private fun <T> pickRandom(vararg options: T): T {
        return options[Random().nextInt(options.size)]
    }

    private fun createDocument(id: Int, value: Int? = id): String = """
        {
            "key": "key-$id",
            "value": "value-$value"
        }
        """



}