package com.sproutsocial.elasticsearch.plugins.eunomia

import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.*
import java.util.concurrent.Callable

class PrioritizingTransportService
    @Inject constructor(settings: Settings,
                        transport: Transport,
                        threadPool: ThreadPool,
                        val priorityDispatcher: PriorityDispatcher)
    : TransportService(settings, transport, threadPool) {

    companion object {
        const val EUNOMIA_PRIORITY_LEVEL_HEADER_KEY = "eunomia-priority-level"
        const val EUNOMIA_PRIORITY_KEY_HEADER_KEY = "eunomia-priority-key"
        const val EUNOMIA_PRIORITY_IN_PROGRESS_HEADER_KEY = "eunomia-priority-in-progress"
    }

    //private lateinit var localNodeId: String

    override fun <REQUEST : TransportRequest> registerRequestHandler(action: String, requestFactory: Callable<REQUEST>, executor: String, handler: TransportRequestHandler<REQUEST>) {
        super.registerRequestHandler(action, requestFactory, executor, handler)

        val realRequestHandler = getRequestHandler(action)
        super.registerRequestHandler(
                "$action[prioritized]",
                requestFactory,
                ThreadPool.Names.SAME,
                PriorityTransportHandler(realRequestHandler))
    }

    override fun <REQUEST : TransportRequest> registerRequestHandler(action: String, request: Class<REQUEST>, executor: String, forceExecution: Boolean, handler: TransportRequestHandler<REQUEST>) {
        super.registerRequestHandler(action, request, executor, forceExecution, handler)
        if (forceExecution) { return }

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

        logger.warn("Routing $action on ${node.id}; priority: ${request.getHeader<String>(EUNOMIA_PRIORITY_LEVEL_HEADER_KEY)}")

        if (!request.hasHeader(EUNOMIA_PRIORITY_LEVEL_HEADER_KEY)) {

            // non prioritized requests are handled as-is. This makes the priority process opt-in
            super.sendRequest(node, action, request, options, handler)
            return
        }

        logger.warn("Prioritizing $action on ${node.id}")
        super.sendRequest(node, "$action[prioritized]", request, options, handler)
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
        override val priorityGroup: String = request.getHeader<String>(EUNOMIA_PRIORITY_KEY_HEADER_KEY) ?: "default"
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

class DelegatingCallbackTransportChannel(
        channel: TransportChannel,
        val callback: () -> Unit) : DelegatingTransportChannel(channel) {
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