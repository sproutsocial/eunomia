package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.block.predicate.Predicate
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.impl.factory.Lists
import java.util.*

class TieredPrioritizedQueue {
    var deadlineInNanos: Long = 10 * 1000 * 1000
    var reprioritizeIntervalInNanos: Long = 1 * 1000 * 1000

    private var nextReprioritizeTimestampNano: Long = 0

    private val tiers: ListIterable<PriorityTier> = Lists.immutable.of(
            PriorityTier(), // very high
            PriorityTier(), // high
            PriorityTier(), // normal
            PriorityTier(), // low
            PriorityTier()) // very low

    fun offer(prioritizedRunnable: PrioritizedRunnable) {
        val targetTier = calculateTierIndex(prioritizedRunnable)
        tiers[targetTier].offer(prioritizedRunnable)
    }

    private fun calculateTierIndex(prioritizedRunnable: PrioritizedRunnable): Int =
            calculateTierIndexFromTimestamp(prioritizedRunnable, System.nanoTime())

    private fun calculateTierIndexFromTimestamp(prioritizedRunnable: PrioritizedRunnable, timestampInNanos: Long): Int {
        val deltaTimeNano = prioritizedRunnable.receivedTimestampInNanos - timestampInNanos
        val priorityShift = (deltaTimeNano / deadlineInNanos).toInt()
        return Math.max(0, prioritizedRunnable.priority - priorityShift)
    }


    fun poll(): PrioritizedRunnable? {
        reprioritize()

        for (tier in tiers) {
            return tier.poll() ?: continue
        }

        return null
    }

    private fun reprioritize() {
        val nowInNanos = System.nanoTime()

        if (nowInNanos < nextReprioritizeTimestampNano) { return }
        nextReprioritizeTimestampNano = nowInNanos + reprioritizeIntervalInNanos

        val reprioritizableRunnables = Lists.mutable.empty<PrioritizedRunnable>()

        for (tierIndex in 1 until tiers.size()) {
            tiers[tierIndex]
                    .extractIf { calculateTierIndexFromTimestamp(it, nowInNanos) != tierIndex }
                    .into(reprioritizableRunnables)
        }

        reprioritizableRunnables.forEach { offer(it) }
    }
}

class PriorityTier {
    private val priorityGroups = Lists.mutable.empty<PriorityGroup>()
    private var nextGroupIndex = 0
    private var shouldOptimize = false

    fun offer(runnable: PrioritizedRunnable) {
        findOrCreatePriorityGroup(runnable.priorityGroup).offer(runnable)
    }

    fun poll(): PrioritizedRunnable? {
        if (priorityGroups.isEmpty) { return null }

        while (true) {
            if (nextGroupIndex >= priorityGroups.size) {
                if (shouldOptimize) { optimize() }
                nextGroupIndex = 0
                shouldOptimize = false

                if (priorityGroups.isEmpty) { return null }
                continue
            }

            val prioritizedRunnable = priorityGroups[nextGroupIndex++].poll()

            when (prioritizedRunnable) {
                null -> shouldOptimize = true
                else -> return prioritizedRunnable
            }
        }
    }

    private fun optimize() {
        priorityGroups.removeIf(Predicate { it.isEmpty })
    }

    private fun findOrCreatePriorityGroup(name: String): PriorityGroup {
        return (priorityGroups
                .firstOrNull { it.name == name }
                ?: createNewPriorityGroup(name))
    }

    private fun createNewPriorityGroup(name: String): PriorityGroup {
        return PriorityGroup(name).also {
            priorityGroups.add(it)
        }
    }

    fun extractIf(filter: (PrioritizedRunnable) -> Boolean): RichIterable<PrioritizedRunnable> {
        return priorityGroups.flatCollect { it.extractIf(filter) }
    }
}

class PriorityGroup(val name: String) {
    private val requestQueue = LinkedList<PrioritizedRunnable>()
    val isEmpty: Boolean get() = requestQueue.isEmpty()

    fun offer(runnable: PrioritizedRunnable) {
        requestQueue.add(runnable)
    }

    fun poll(): PrioritizedRunnable? = requestQueue.pollFirst()

    fun extractIf(filter: (PrioritizedRunnable) -> Boolean): RichIterable<PrioritizedRunnable> {
        return requestQueue.extractIf(filter)
    }
}

private fun <T> MutableIterable<T>.extractIf(filter: (T) -> Boolean): RichIterable<T> {
    val iterator = this.iterator()
    val result = Lists.mutable.empty<T>()

    while (iterator.hasNext()) {
        val value = iterator.next()

        if (filter.invoke(value)) {
            result.add(value)
            iterator.remove()
        }
    }

    return result
}
