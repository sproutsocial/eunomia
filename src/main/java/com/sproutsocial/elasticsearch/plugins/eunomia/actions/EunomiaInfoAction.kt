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

import org.eclipse.collections.impl.factory.Lists
import org.elasticsearch.action.Action
import org.elasticsearch.action.support.nodes.*
import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.cluster.ClusterName
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.io.stream.Streamable

class EunomiaInfoAction
    : Action<EunomiaInfoRequest, EunomiaInfoResponse, EunomiaInfoRequestBuilder>(NAME) {

    companion object {
        val INSTANCE = EunomiaInfoAction()
        const val NAME = "cluster:monitor/eunomia"
    }

    override fun newResponse(): EunomiaInfoResponse = EunomiaInfoResponse()

    override fun newRequestBuilder(client: ElasticsearchClient): EunomiaInfoRequestBuilder {
        return EunomiaInfoRequestBuilder(client, this)
    }
}

class EunomiaInfoRequest: BaseNodesRequest<EunomiaInfoRequest>() {

}

class EunomiaInfoNodeRequest: BaseNodeRequest {
    constructor() : super()
    constructor(request: EunomiaInfoRequest, nodeId: String) : super(request, nodeId)
}

class EunomiaInfoNodeResponse: BaseNodeResponse {
    constructor() : super()
    constructor(localNode: DiscoveryNode) : super(localNode)

    var prioritizedActionEntries: MutableList<PrioritizedActionEntry> = Lists.mutable.empty<PrioritizedActionEntry>()

    override fun readFrom(streamInput: StreamInput) {
        super.readFrom(streamInput)
        for(i in 0 until streamInput.readVInt()) {
            val nextEntry = PrioritizedActionEntry().also { it.readFrom(streamInput) }
            prioritizedActionEntries.add(nextEntry)
        }
    }

    override fun writeTo(streamOutput: StreamOutput) {
        super.writeTo(streamOutput)
        streamOutput.writeVInt(prioritizedActionEntries.size)
        prioritizedActionEntries.forEach {
            it.writeTo(streamOutput)
        }
    }
}

class PrioritizedActionEntry: Streamable {
    var state: String? = null
    var actionName: String? = null
    var requestedPriority: Int? = null
    var priorityTier: String? = null
    var priorityGroup: String? = null
    var inFlight: Boolean? = null
    var receivedTimestampInNanos: Long = 0
    var executor: String? = null

    override fun readFrom(streamInput: StreamInput) {
        state = streamInput.readOptionalString()
        actionName = streamInput.readOptionalString()
        requestedPriority = streamInput.readOptionalVInt()
        priorityTier = streamInput.readOptionalString()
        priorityGroup = streamInput.readOptionalString()
        inFlight = streamInput.readOptionalBoolean()
        receivedTimestampInNanos = streamInput.readLong()
        executor = streamInput.readOptionalString()
    }

    override fun writeTo(streamOutput: StreamOutput) {
        streamOutput.writeOptionalString(state)
        streamOutput.writeOptionalString(actionName)
        streamOutput.writeOptionalVInt(requestedPriority)
        streamOutput.writeOptionalString(priorityTier)
        streamOutput.writeOptionalString(priorityGroup)
        streamOutput.writeOptionalBoolean(inFlight)
        streamOutput.writeLong(receivedTimestampInNanos)
        streamOutput.writeOptionalString(executor)
    }

}

class EunomiaInfoResponse(
        clusterName: ClusterName? = null,
        val nodeResponses: Array<EunomiaInfoNodeResponse>? = null)
    : BaseNodesResponse<EunomiaInfoNodeResponse>(clusterName, nodeResponses) {
}

class EunomiaInfoRequestBuilder(
        client: ElasticsearchClient,
        action: EunomiaInfoAction)
    : NodesOperationRequestBuilder<EunomiaInfoRequest, EunomiaInfoResponse, EunomiaInfoRequestBuilder>(client, action, EunomiaInfoRequest()) {
}

