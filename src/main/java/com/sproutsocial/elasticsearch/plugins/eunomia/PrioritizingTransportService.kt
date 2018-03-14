package com.sproutsocial.elasticsearch.plugins.eunomia

import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.node.settings.NodeSettingsService
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.*
import java.util.concurrent.Callable

class PrioritizingTransportService
    @Inject constructor(settings: Settings,
                        settingsService: NodeSettingsService,
                        transport: Transport,
                        threadPool: ThreadPool,
                        val priorityDispatcher: PriorityDispatcher)
    : TransportService(settings, transport, threadPool) {

    companion object {
        const val EUNOMIA_PRIORITY_LEVEL_HEADER_KEY = "eunomia-priority-level"
        const val EUNOMIA_PRIORITY_KEY_HEADER_KEY = "eunomia-priority-key"
        const val EUNOMIA_PRIORITY_KEY_DEFAULT_VALUE = "_default_"
        const val EUNOMIA_PRIORITY_IN_PROGRESS_HEADER_KEY = "eunomia-priority-in-progress"
        const val SETTINGS_IS_ENABLED_KEY = "eunomia.requestPrioritizing.enabled"
    }

    private var effectiveSettings: Settings = settings
    private var isEnabled = true

    init {
        updateComponentSettings(settings)
        settingsService.addListener { newSettings ->
            effectiveSettings = Settings.builder().put(effectiveSettings).put(newSettings).build()
            updateComponentSettings(effectiveSettings)
        }
    }

    private fun updateComponentSettings(settings: Settings) {
        isEnabled = settings.getAsBoolean(SETTINGS_IS_ENABLED_KEY, true)
    }

    override fun <REQUEST : TransportRequest> registerRequestHandler(action: String, requestFactory: Callable<REQUEST>, executor: String, handler: TransportRequestHandler<REQUEST>) {
        super.registerRequestHandler(action, requestFactory, executor, handler)

        // there is no point in prioritizing actions that are intended to be run in-line with the current requests;
        // also, these requests tend to be very low CPU and low latency
        if (executor == ThreadPool.Names.SAME) { return }

        val realRequestHandler = getRequestHandler(action)
        super.registerRequestHandler(
                "$action[prioritized]",
                requestFactory,
                ThreadPool.Names.SAME,
                PriorityTransportHandler(realRequestHandler))
    }

    override fun <REQUEST : TransportRequest> registerRequestHandler(action: String, request: Class<REQUEST>, executor: String, forceExecution: Boolean, handler: TransportRequestHandler<REQUEST>) {
        super.registerRequestHandler(action, request, executor, forceExecution, handler)

        // there is no point in prioritizing actions that are intended to be run in-line with the current requests;
        // also, these requests tend to be very low CPU and low latency. Also avoid forceExecutions since they need
        // to be ran immediately and tend to be related to data consistency
        if (forceExecution || executor == ThreadPool.Names.SAME) { return }

        val realRequestHandler = getRequestHandler(action)
        super.registerRequestHandler(
                "$action[prioritized]",
                request,
                ThreadPool.Names.SAME,
                false,
                PriorityTransportHandler(realRequestHandler))
    }

    override fun <T : TransportResponse> sendRequest(
            node: DiscoveryNode,
            action: String,
            request: TransportRequest,
            options: TransportRequestOptions,
            handler: TransportResponseHandler<T>) {

        if (!isEnabled || !request.hasHeader(EUNOMIA_PRIORITY_LEVEL_HEADER_KEY)) {
            // non prioritized requests are handled as-is. This makes the priority process opt-in
            super.sendRequest(node, action, request, options, handler)
            return
        }

        val newActionName = "$action[prioritized]"
        if (getRequestHandler(newActionName) == null) {
            // prevent NPE for actions that couldn't be prioritized (see registerRequestHandler for context).
            // This is a defensive check since this shouldn't happen for normal use cases.
            super.sendRequest(node, action, request, options, handler)
            return
        }

        logPrioritizedRequest(action, node, request)
        super.sendRequest(node, newActionName, request, options, handler)
    }

    private fun logPrioritizedRequest(action: String, node: DiscoveryNode, request: TransportRequest) {
        if (!logger.isDebugEnabled) { return }
        
        logger.debug("Prioritizing {} on {} with {} priority in group {}",
                action,
                node.name,
                request.getHeader(EUNOMIA_PRIORITY_LEVEL_HEADER_KEY),
                request.getHeader(EUNOMIA_PRIORITY_IN_PROGRESS_HEADER_KEY) ?: EUNOMIA_PRIORITY_KEY_DEFAULT_VALUE)
    }

    inner class PriorityTransportHandler<REQUEST : TransportRequest>(
            private val realRequestHandler: RequestHandlerRegistry<TransportRequest>)
        : TransportRequestHandler<REQUEST>() {

        override fun messageReceived(request: REQUEST, channel: TransportChannel) {
            logger.warn("priority message received ${request.headers}")
            priorityDispatcher.schedule(DelegatingCommand(realRequestHandler, request, channel))
        }
    }

    inner class DelegatingCommand <REQUEST : TransportRequest> (
            private val realRequestHandler: RequestHandlerRegistry<REQUEST>,
            private val request: REQUEST,
            private val channel: TransportChannel) : PrioritizedRunnable {
        override val inFlight: Boolean = request.getHeader<Boolean>(EUNOMIA_PRIORITY_IN_PROGRESS_HEADER_KEY) ?: false
        override val executor: String = realRequestHandler.executor
        override val priority: Int = parseRequestPriority(request)
        override val priorityGroup: String = request.getHeader<String>(EUNOMIA_PRIORITY_KEY_HEADER_KEY) ?: EUNOMIA_PRIORITY_KEY_DEFAULT_VALUE
        override val receivedTimestampInNanos: Long = System.nanoTime()

        override fun run(onComplete: () -> Unit) {
            val delegateChannel = DelegatingCallbackTransportChannel(channel, onComplete)

            try {
                request.putHeader(EUNOMIA_PRIORITY_IN_PROGRESS_HEADER_KEY, true)
                realRequestHandler.processMessageReceived(request, delegateChannel)
            }
            catch (ex: Exception) {
                try {
                    delegateChannel.sendResponse(ex)
                } catch (e1: Exception) {
                    logger.warn("failed to notify channel of error message for action [${realRequestHandler.action}]", e1)
                    logger.warn("actual exception", ex)
                }
            }
        }

        override fun toString(): String {
            return "DelegatingCommand(request=$request, inFlight=$inFlight, executor='$executor', priority=$priority, priorityGroup='$priorityGroup', receivedTimestampInNanos=$receivedTimestampInNanos)"
        }
    }

    private fun parseRequestPriority(request: TransportRequest): Int {
        val priorityStringValue = request.getHeader<String>(EUNOMIA_PRIORITY_LEVEL_HEADER_KEY)

        return when (priorityStringValue) {
            "very high" -> 0
            "high" -> 1
            "normal" -> 2
            "low" -> 3
            "very low" -> 4
            else -> throw IllegalArgumentException("could not parse $EUNOMIA_PRIORITY_LEVEL_HEADER_KEY: $priorityStringValue")
        }
    }
}

/**
 * This class is used to determine when a task completes. Since tasks can spawn sub tasks or delegate to new threads,
 * we can't just wait on the method to return. Tracking the response seems to be how elasticsearch-core does it too.
 * See org.elasticsearch.transport.RequestHandlerRegistry.TransportChannelWrapper and
 * org.elasticsearch.transport.RequestHandlerRegistry.processMessageReceived
 */
class DelegatingCallbackTransportChannel(
        channel: TransportChannel,
        private val callback: () -> Unit) : DelegatingTransportChannel(channel) {

    override fun sendResponse(response: TransportResponse) {
        callback.invoke()
        super.sendResponse(response)
    }

    override fun sendResponse(response: TransportResponse, options: TransportResponseOptions) {
        callback.invoke()
        super.sendResponse(response, options)
    }

    override fun sendResponse(error: Throwable) {
        callback.invoke()
        super.sendResponse(error)
    }
}