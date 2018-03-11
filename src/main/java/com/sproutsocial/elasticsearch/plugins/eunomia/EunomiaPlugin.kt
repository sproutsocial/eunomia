package com.sproutsocial.elasticsearch.plugins.eunomia

import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.TransportModule


class EunomiaPlugin: Plugin() {
    override fun name() = "eunomia"

    override fun description() = "thread priority gateway"

    override fun nodeModules() = mutableListOf(EunomiaModule())

    fun onModule(transportModule: TransportModule) {
        transportModule.addTransportService("prioritizing-transport-service", PrioritizingTransportService::class.java)
    }
}