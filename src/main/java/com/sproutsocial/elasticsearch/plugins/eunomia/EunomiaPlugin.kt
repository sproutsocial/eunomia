package com.sproutsocial.elasticsearch.plugins.eunomia

import com.sproutsocial.elasticsearch.plugins.eunomia.actions.EunomiaInfoAction
import com.sproutsocial.elasticsearch.plugins.eunomia.actions.EunomiaInfoTransportAction
import org.elasticsearch.action.ActionModule
import org.elasticsearch.cluster.ClusterModule
import org.elasticsearch.cluster.settings.Validator
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.TransportModule


class EunomiaPlugin: Plugin() {
    override fun name() = "eunomia"

    override fun description() = "thread priority gateway"

    override fun nodeModules() = mutableListOf(EunomiaModule())

    fun onModule(transportModule: TransportModule) {
        transportModule.addTransportService("prioritizing-transport-service", PrioritizingTransportService::class.java)
    }

    fun onModule(clusterModule: ClusterModule) {
        clusterModule.registerClusterDynamicSetting(PrioritizedGroupThrottler.SETTINGS_MAX_GROUP_SIZE_KEY, Validator.POSITIVE_INTEGER)
        clusterModule.registerClusterDynamicSetting(PrioritizedGroupThrottler.Companion.SETTINGS_IS_ENABLED_KEY, Validator.BOOLEAN)
        clusterModule.registerClusterDynamicSetting(PrioritizingTransportService.Companion.SETTINGS_IS_ENABLED_KEY, Validator.BOOLEAN)
        clusterModule.registerClusterDynamicSetting(PriorityDispatcher.SETTINGS_TARGET_ACTIVE_REQUESTS_KEY, Validator.POSITIVE_INTEGER)
        clusterModule.registerClusterDynamicSetting(PriorityDispatcher.SETTINGS_DEADLINE_KEY, Validator.POSITIVE_INTEGER)
    }

    fun onModule(actionModule: ActionModule) {
        actionModule.registerAction(EunomiaInfoAction.INSTANCE, EunomiaInfoTransportAction::class.java)
    }
}