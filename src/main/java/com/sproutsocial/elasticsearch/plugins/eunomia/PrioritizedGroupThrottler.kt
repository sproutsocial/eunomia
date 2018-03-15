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

package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.impl.factory.primitive.ObjectIntMaps
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.node.settings.NodeSettingsService

class PrioritizedGroupThrottler @Inject constructor(
        settings: Settings,
        settingsService: NodeSettingsService)
    : AbstractComponent(settings) {

    companion object {
        const val SETTINGS_IS_ENABLED_KEY = "eunomia.groupThrottling.enabled"
        const val SETTINGS_MAX_GROUP_SIZE_KEY = "eunomia.groupThrottling.maxGroupSize"
    }

    private var effectiveSettings: Settings = settings
    private val groupRequestQueueSizeMap = ObjectIntMaps.mutable.empty<String>()
    private var isEnabled = true
    private var maxGroupSize = 1000

    init {
        updateComponentSettings(settings)
        settingsService.addListener { newSettings ->
            effectiveSettings = Settings.builder().put(effectiveSettings).put(newSettings).build()
            updateComponentSettings(effectiveSettings)
        }
    }

    private fun updateComponentSettings(settings: Settings) {
        isEnabled = settings.getAsBoolean(SETTINGS_IS_ENABLED_KEY, true)
        maxGroupSize = settings.getAsInt(SETTINGS_MAX_GROUP_SIZE_KEY, 1000)

        if (!isEnabled) {
            groupRequestQueueSizeMap.clear()
        }
    }

    fun offer(prioritizedRunnable: PrioritizedRunnable) {
        if (!isEnabled) { return }

        groupRequestQueueSizeMap.updateValue(prioritizedRunnable.priorityGroup, 0, {it + 1}).also {
            doThrottleCheck(prioritizedRunnable, it)
        }
    }

    private fun doThrottleCheck(prioritizedRunnable: PrioritizedRunnable, groupSize: Int) {
        if (groupSize >= maxGroupSize) {
            logger.warn("rejecting ${prioritizedRunnable.action} for ${prioritizedRunnable.priorityGroup} because the the priority group is full")
            throw EsRejectedExecutionException("could not run $prioritizedRunnable because group size for ${prioritizedRunnable.priorityGroup} has exceeded $maxGroupSize")
        }
    }

    fun completed(prioritizedRunnable: PrioritizedRunnable) {
        if (!isEnabled) { return }

        groupRequestQueueSizeMap.updateValue(prioritizedRunnable.priorityGroup, 0, {it - 1}).also {
            if (it <= 0L) { groupRequestQueueSizeMap.remove(prioritizedRunnable.priorityGroup) }
        }
    }
}