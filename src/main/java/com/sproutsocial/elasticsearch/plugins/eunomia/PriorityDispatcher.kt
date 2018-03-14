package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.impl.factory.Lists
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.EsExecutors
import org.elasticsearch.node.settings.NodeSettingsService
import org.elasticsearch.threadpool.ThreadPool

class PriorityDispatcher
    @Inject constructor(
            settings: Settings,
            settingsService: NodeSettingsService,
            private val threadPool: ThreadPool,
            private val groupThrottler: PrioritizedGroupThrottler)
    : AbstractComponent(settings) {

    companion object {
        const val SETTINGS_MAX_ACTIVE_THREADS_KEY = "eunomia.priorityDispatcher.maxActiveThreads"
    }

    private val pendingQueue = TieredPrioritizedQueue()
    private val activeRunnables = Lists.mutable.empty<PrioritizedRunnable>()
    private var maxActiveThreads = Math.max(1, EsExecutors.boundedNumberOfProcessors(settings) - 2)
    private var effectiveSettings: Settings = settings

    init {
        updateComponentSettings(settings)
        settingsService.addListener { newSettings ->
            effectiveSettings = Settings.builder().put(effectiveSettings).put(newSettings).build()
            updateComponentSettings(effectiveSettings)
        }
    }

    private fun updateComponentSettings(settings: Settings) {
        maxActiveThreads = settings.getAsInt(SETTINGS_MAX_ACTIVE_THREADS_KEY, Math.max(1, EsExecutors.boundedNumberOfProcessors(settings) - 2))
    }

    @Synchronized
    fun schedule(prioritizedRunnable: PrioritizedRunnable) {
        groupThrottler.offer(prioritizedRunnable)

        if (prioritizedRunnable.inFlight) {
            runRunnable(prioritizedRunnable)
        }
        else {
            pendingQueue.offer(prioritizedRunnable)
            doScheduling()
        }
    }

    private fun doScheduling() {
        while (activeRunnables.size < maxActiveThreads) {
            val nextRunnable = pendingQueue.poll() ?: return

            runRunnable(nextRunnable)
        }
    }

    private fun runRunnable(nextRunnable: PrioritizedRunnable) {
        activeRunnables.add(nextRunnable)
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
        activeRunnables.remove(prioritizedRunnable)
        doScheduling()
    }
}