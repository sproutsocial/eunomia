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

package com.sproutsocial.elasticsearch.plugins.eunomia.handlers

import com.sproutsocial.elasticsearch.plugins.eunomia.actions.EunomiaInfoAction
import com.sproutsocial.elasticsearch.plugins.eunomia.actions.EunomiaInfoResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.Table
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.rest.RestChannel
import org.elasticsearch.rest.RestController
import org.elasticsearch.rest.RestRequest
import org.elasticsearch.rest.RestResponse
import org.elasticsearch.rest.action.cat.AbstractCatAction
import org.elasticsearch.rest.action.support.RestResponseListener
import org.elasticsearch.rest.action.support.RestTable
import java.lang.StringBuilder

class EunomiaInfoCatHandler
@Inject constructor(settings: Settings,
                    restController: RestController,
                    client: Client) :
        AbstractCatAction(settings, restController, client) {

    init {
        restController.registerHandler(RestRequest.Method.GET, "/_cat/prioritized_actions", this)
    }

    override fun documentation(sb: StringBuilder) {
        sb.append("/_cat/prioritized_actions\n")
    }

    override fun getTableWithHeader(request: RestRequest): Table {
        return Table().apply {
            this.startHeaders()
            this.addCell("ip", "default:true;desc:ip of node where it lives")
            this.addCell("id", "default:true;desc:unique id of node where it lives")
            this.addCell("node", "default:true;alias:n;desc:name of node where it lives")
            this.addCell("state", "desc:request state")
            this.addCell("actionName", "desc:action name")
            this.addCell("requestedPriority", "desc:requested priority")
            this.addCell("priorityTier", "desc:current priority tier")
            this.addCell("priorityGroup", "desc:priority group")
            this.addCell("inFlight", "desc:is in progress")
            this.addCell("receivedTimestampInNanos", "desc: received timestamp (ns)")
            this.addCell("executor", "desc: target tread pool")
            this.endHeaders()
        }
    }

    override fun doRequest(request: RestRequest, channel: RestChannel, client: Client) {
        val internalRequest = EunomiaInfoAction.INSTANCE
                .newRequestBuilder(client)
                .setNodesIds("_all")
                .request()

        client.execute(EunomiaInfoAction.INSTANCE, internalRequest, object : RestResponseListener<EunomiaInfoResponse>(channel) {
            override fun buildResponse(response: EunomiaInfoResponse): RestResponse {
                return getTableWithHeader(request).apply {
                    response.nodeResponses?.forEach { nodeResponse ->
                        nodeResponse.prioritizedActionEntries.forEach { entry ->
                            this.startRow()
                            this.addCell(nodeResponse.node.hostAddress)
                            this.addCell(nodeResponse.node.id())
                            this.addCell(nodeResponse.node.hostName)
                            this.addCell(entry.state)
                            this.addCell(entry.actionName)
                            this.addCell(entry.requestedPriority)
                            this.addCell(entry.priorityTier ?: "-")
                            this.addCell(entry.priorityGroup)
                            this.addCell(entry.inFlight ?: false)
                            this.addCell(entry.receivedTimestampInNanos)
                            this.addCell(entry.executor ?: "<unknown>")
                            this.endRow() } } }
                        .run { RestTable.buildResponse(this, channel) }
            }
        })


    }
}