package com.sproutsocial.elasticsearch.plugins.eunomia

import org.elasticsearch.common.inject.AbstractModule

class EunomiaModule : AbstractModule() {
    override fun configure() {
        bind(PriorityDispatcher::class.java).asEagerSingleton()
    }

}
