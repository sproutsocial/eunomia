/*
 * The MIT License (MIT)
 * Copyright (c) 2018 SproutSocial, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in
 *  all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 *  THE SOFTWARE.
 */

package com.sproutsocial.elasticsearch.plugins.eunomia.actions

import com.sproutsocial.elasticsearch.plugins.eunomia.PrioritizedRunnable
import com.sproutsocial.elasticsearch.plugins.eunomia.PriorityDispatcher
import org.elasticsearch.action.support.ActionFilters
import org.elasticsearch.action.support.nodes.TransportNodesAction
import org.elasticsearch.cluster.ClusterName
import org.elasticsearch.cluster.ClusterService
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.threadpool.ThreadPool
import org.elasticsearch.transport.TransportService
import java.util.concurrent.atomic.AtomicReferenceArray

class EunomiaInfoTransportAction
@Inject constructor(private val priorityDispatcher: PriorityDispatcher,
                    settings: Settings,
                    clusterName: ClusterName,
                    threadPool: ThreadPool,
                    clusterService: ClusterService,
                    transportService: TransportService,
                    actionFilters: ActionFilters,
                    indexNameExpressionResolver: IndexNameExpressionResolver)
    : TransportNodesAction<EunomiaInfoRequest, EunomiaInfoResponse, EunomiaInfoNodeRequest, EunomiaInfoNodeResponse>(
        settings, EunomiaInfoAction.NAME, clusterName, threadPool, clusterService, transportService, actionFilters,
        indexNameExpressionResolver, EunomiaInfoRequest::class.java, EunomiaInfoNodeRequest::class.java,
        ThreadPool.Names.GENERIC) {

    override fun newNodeRequest(nodeId: String, request: EunomiaInfoRequest): EunomiaInfoNodeRequest =
            EunomiaInfoNodeRequest(request, nodeId)

    override fun newNodeResponse(): EunomiaInfoNodeResponse = EunomiaInfoNodeResponse()

    override fun newResponse(request: EunomiaInfoRequest, nodesResponses: AtomicReferenceArray<*>): EunomiaInfoResponse {
        val nodeResponses = nodesResponses.mapIf({ it is EunomiaInfoNodeResponse }, { it as EunomiaInfoNodeResponse })
        return EunomiaInfoResponse(clusterName, nodeResponses.toTypedArray())
    }

    override fun accumulateExceptions(): Boolean = false

    override fun nodeOperation(request: EunomiaInfoNodeRequest): EunomiaInfoNodeResponse {
        return EunomiaInfoNodeResponse(clusterService.localNode()).apply {
            priorityDispatcher
                    .activeRunables
                    .collect {
                        PrioritizedActionEntry().apply {
                            this.state = "RUNNING"
                            it.mapTo(this) } }
                    .forEach { this.prioritizedActionEntries.add(it) }

            priorityDispatcher
                    .pendingQueue
                    .tiers
                    .flatCollect {tier ->
                        tier.priorityGroups.flatCollect { group ->
                            group.requestQueue.map {runnable ->
                                PrioritizedActionEntry().apply {
                                    this.state = "PENDING"
                                    this.priorityTier = tier.name
                                    this.priorityGroup = group.name
                                    runnable.mapTo(this) } } } }
                    .forEach { this.prioritizedActionEntries.add(it) }
        }

    }

}

private fun PrioritizedRunnable.mapTo(prioritizedActionEntry: PrioritizedActionEntry) {
    prioritizedActionEntry.actionName = this.action
    prioritizedActionEntry.executor = this.executor
    prioritizedActionEntry.inFlight = this.inFlight
    prioritizedActionEntry.priorityGroup = this.priorityGroup
    prioritizedActionEntry.requestedPriority = this.priority
    prioritizedActionEntry.receivedTimestampInNanos = this.receivedTimestampInNanos
}

inline fun <T, R> AtomicReferenceArray<T>.mapIf(predicate: (T) -> Boolean, transform: (T) -> R): List<R> {
    return mapToIf(ArrayList(this.length()), predicate, transform)
}

inline fun <T, R, C : MutableCollection<in R>> AtomicReferenceArray<T>.mapToIf(
        destination: C, predicate: (T) -> Boolean, transform: (T) -> R): C {

    for (i in 0 until this.length()) {
        val value = this[i]

        if (predicate.invoke(value))
            destination.add(transform(value))
    }

    return destination
}