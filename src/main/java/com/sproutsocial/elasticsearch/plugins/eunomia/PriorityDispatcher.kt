package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.impl.factory.Lists
import org.elasticsearch.common.component.AbstractComponent
import org.elasticsearch.common.inject.Inject
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.threadpool.ThreadPool

class PriorityDispatcher
    @Inject constructor(
            settings: Settings,
            private val threadPool: ThreadPool)
    : AbstractComponent(settings) {

    private val pendingQueue = TieredPrioritizedQueue()
    private val activeRunnables = Lists.mutable.empty<PrioritizedRunnable>()
    private val maxActiveThreads = 4

    @Synchronized fun schedule(prioritizedRunnable: PrioritizedRunnable) {
        pendingQueue.offer(prioritizedRunnable)
        runPendingTasks()
    }

    private fun runPendingTasks() {
        while (activeRunnables.size < maxActiveThreads) {
            val nextRunnable = pendingQueue.poll() ?: return

            activeRunnables.add(nextRunnable)
            threadPool.executor(nextRunnable.executor).execute {
                try {
                    nextRunnable.run(onComplete = {completeRunnable(nextRunnable)})
                }
                catch (ex: Exception) {
                    completeRunnable(nextRunnable) 
                }
            }
        }
    }

    @Synchronized fun completeRunnable(prioritizedRunnable: PrioritizedRunnable) {
        activeRunnables.remove(prioritizedRunnable)
        runPendingTasks()
    }


}