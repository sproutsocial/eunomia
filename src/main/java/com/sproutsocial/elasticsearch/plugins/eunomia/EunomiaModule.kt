package com.sproutsocial.elasticsearch.plugins.eunomia

import com.sproutsocial.elasticsearch.plugins.eunomia.PrioritizedGroupThrottler.Companion.SETTINGS_MAX_GROUP_SIZE_KEY
import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.cluster.settings.Validator
import org.elasticsearch.common.inject.AbstractModule

class EunomiaModule : AbstractModule() {
    override fun configure() {
        bind(PriorityDispatcher::class.java).asEagerSingleton()
        bind(PrioritizedGroupThrottler::class.java).asEagerSingleton()
    }
    
    fun onModule(clusterModule: ClusterModule) {
        clusterModule.registerClusterDynamicSetting(SETTINGS_MAX_GROUP_SIZE_KEY, Validator.POSITIVE_INTEGER)
        clusterModule.registerClusterDynamicSetting(PrioritizedGroupThrottler.Companion.SETTINGS_IS_ENABLED_KEY, Validator.BOOLEAN)
        clusterModule.registerClusterDynamicSetting(PrioritizingTransportService.Companion.SETTINGS_IS_ENABLED_KEY, Validator.BOOLEAN)
        clusterModule.registerClusterDynamicSetting(PriorityDispatcher.SETTINGS_MAX_ACTIVE_THREADS_KEY, Validator.POSITIVE_INTEGER)
    }
}
