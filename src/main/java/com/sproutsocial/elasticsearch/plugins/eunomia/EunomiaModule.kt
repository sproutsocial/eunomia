package com.sproutsocial.elasticsearch.plugins.eunomia

import com.sproutsocial.elasticsearch.plugins.eunomia.handlers.EunomiaInfoCatHandler
import org.elasticsearch.common.inject.AbstractModule

class EunomiaModule : AbstractModule() {
    override fun configure() {
        bind(PriorityDispatcher::class.java).asEagerSingleton()
        bind(PrioritizedGroupThrottler::class.java).asEagerSingleton()
        bind(EunomiaInfoCatHandler::class.java).asEagerSingleton()
    }
}
