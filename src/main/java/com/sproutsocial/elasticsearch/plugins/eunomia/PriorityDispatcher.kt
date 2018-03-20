package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.impl.factory.Lists
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.EsExecutors
import org.elasticsearch.node.settings.NodeSettingsService
import org.elasticsearch.threadpool.ThreadPool
import java.util.concurrent.TimeUnit

class PriorityDispatcher
    @Inject constructor(
            settings: Settings,
            settingsService: NodeSettingsService,
            private val threadPool: ThreadPool,
            private val groupThrottler: PrioritizedGroupThrottler)
    : AbstractComponent(settings) {

    companion object {
        const val SETTINGS_TARGET_ACTIVE_REQUESTS_KEY = "eunomia.priorityDispatcher.targetActiveRequests"
        const val SETTINGS_DEADLINE_KEY = "eunomia.priorityDispatcher.deadlineInSeconds"
    }

    private val _activeRunnables = Lists.mutable.empty<PrioritizedRunnable>()
    val activeRunables: ListIterable<PrioritizedRunnable> get() = _activeRunnables

    private var targetActiveThreads = Math.max(1, EsExecutors.boundedNumberOfProcessors(settings) - 2)
    private var effectiveSettings: Settings = settings

    private val _pendingQueue = TieredPrioritizedQueue(activeRunables, targetActiveThreads)
    val pendingQueue: TieredPrioritizedQueueView get() = _pendingQueue

    init {
        updateComponentSettings(settings)
        settingsService.addListener { newSettings ->
            effectiveSettings = Settings.builder().put(effectiveSettings).put(newSettings).build()
            updateComponentSettings(effectiveSettings)
        }
    }

    private fun updateComponentSettings(settings: Settings) {
        targetActiveThreads = settings.getAsInt(SETTINGS_TARGET_ACTIVE_REQUESTS_KEY, Math.max(1, EsExecutors.boundedNumberOfProcessors(settings) - 2))
        _pendingQueue.deadlineInNanos = TimeUnit.SECONDS.toNanos(settings.getAsLong(SETTINGS_DEADLINE_KEY, 10))
        _pendingQueue.targetActiveThreads = targetActiveThreads
    }

    @Synchronized
    fun schedule(prioritizedRunnable: PrioritizedRunnable) {
        groupThrottler.offer(prioritizedRunnable)

        _pendingQueue.offer(prioritizedRunnable)
        doScheduling()
    }

    private fun doScheduling() {
        while (true) {
            val nextRunnable = _pendingQueue.poll() ?: return

            runRunnable(nextRunnable)
        }
    }

    private fun runRunnable(nextRunnable: PrioritizedRunnable) {
        _activeRunnables.add(nextRunnable)
        threadPool.executor(nextRunnable.executor).execute {
            try {
                nextRunnable.run(onComplete = { completeRunnable(nextRunnable) })
            } catch (ex: Exception) {
                completeRunnable(nextRunnable)
            }
        }
    }

    @Synchronized
    private fun completeRunnable(prioritizedRunnable: PrioritizedRunnable) {
        groupThrottler.completed(prioritizedRunnable)
        _activeRunnables.remove(prioritizedRunnable)
        doScheduling()
    }
}