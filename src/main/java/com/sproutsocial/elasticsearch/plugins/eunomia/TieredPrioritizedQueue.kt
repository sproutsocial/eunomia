package com.sproutsocial.elasticsearch.plugins.eunomia

import org.eclipse.collections.api.RichIterable
import org.eclipse.collections.api.block.predicate.Predicate
import org.eclipse.collections.api.list.ListIterable
import org.eclipse.collections.impl.factory.Lists
import java.util.*
import java.util.concurrent.TimeUnit

interface TieredPrioritizedQueueView {
    val tiers: ListIterable<PriorityTierView>
}

class TieredPrioritizedQueue : TieredPrioritizedQueueView {
    var deadlineInNanos: Long = TimeUnit.SECONDS.toNanos(10)
    var reprioritizeIntervalInNanos: Long = TimeUnit.SECONDS.toNanos(1)

    private var nextReprioritizeTimestampNano: Long = 0

    private val _tiers: ListIterable<PriorityTier> = Lists.immutable.of(
            PriorityTier("very high"),
            PriorityTier("high"),
            PriorityTier("normal"),
            PriorityTier("low"),
            PriorityTier("very low"))

    @Suppress("UNCHECKED_CAST")
    override val tiers: ListIterable<PriorityTierView> get() = _tiers as ListIterable<PriorityTierView>


    fun offer(prioritizedRunnable: PrioritizedRunnable) {
        val targetTier = calculateTierIndex(prioritizedRunnable)
        _tiers[targetTier].offer(prioritizedRunnable)
    }

    private fun calculateTierIndex(prioritizedRunnable: PrioritizedRunnable): Int =
            calculateTierIndexFromTimestamp(prioritizedRunnable, System.nanoTime())

    private fun calculateTierIndexFromTimestamp(prioritizedRunnable: PrioritizedRunnable, timestampInNanos: Long): Int {
        val deltaTimeNano = timestampInNanos - prioritizedRunnable.receivedTimestampInNanos
        val priorityShift = (deltaTimeNano / deadlineInNanos).toInt()
        return Math.max(0, prioritizedRunnable.priority - priorityShift)
    }


    fun poll(): PrioritizedRunnable? {
        reprioritize()

        for (tier in _tiers) {
            return tier.poll() ?: continue
        }

        return null
    }

    private fun reprioritize() {
        val nowInNanos = System.nanoTime()

        if (nowInNanos < nextReprioritizeTimestampNano) { return }
        nextReprioritizeTimestampNano = nowInNanos + reprioritizeIntervalInNanos

        val reprioritizableRunnables = Lists.mutable.empty<PrioritizedRunnable>()

        for (tierIndex in 1 until _tiers.size()) {
            _tiers[tierIndex]
                    .extractIf { calculateTierIndexFromTimestamp(it, nowInNanos) != tierIndex }
                    .into(reprioritizableRunnables)
        }

        reprioritizableRunnables.forEach { offer(it) }
    }
}

interface PriorityTierView {
    val name: String
    val priorityGroups: ListIterable<PriorityGroupView>
}



class PriorityTier(override val name: String) : PriorityTierView {
    private val _priorityGroups = Lists.mutable.empty<PriorityGroup>()

    @Suppress("UNCHECKED_CAST")
    override val priorityGroups: ListIterable<PriorityGroupView> get() = _priorityGroups as ListIterable<PriorityGroupView>

    private var nextGroupIndex = 0
    private var shouldOptimize = false

    fun offer(runnable: PrioritizedRunnable) {
        findOrCreatePriorityGroup(runnable.priorityGroup).offer(runnable)
    }

    fun poll(): PrioritizedRunnable? {
        if (_priorityGroups.isEmpty) { return null }

        while (true) {
            if (nextGroupIndex >= _priorityGroups.size) {
                if (shouldOptimize) { optimize() }
                nextGroupIndex = 0
                shouldOptimize = false

                if (_priorityGroups.isEmpty) { return null }
                continue
            }

            val prioritizedRunnable = _priorityGroups[nextGroupIndex++].poll()

            when (prioritizedRunnable) {
                null -> shouldOptimize = true
                else -> return prioritizedRunnable
            }
        }
    }

    private fun optimize() {
        _priorityGroups.removeIf(Predicate { it.isEmpty })
    }

    private fun findOrCreatePriorityGroup(name: String): PriorityGroup {
        return (_priorityGroups
                .firstOrNull { it.name == name }
                ?: createNewPriorityGroup(name))
    }

    private fun createNewPriorityGroup(name: String): PriorityGroup {
        return PriorityGroup(name).also {
            _priorityGroups.add(it)
        }
    }

    fun extractIf(filter: (PrioritizedRunnable) -> Boolean): RichIterable<PrioritizedRunnable> {
        return _priorityGroups.flatCollect { it.extractIf(filter) }
    }
}

interface PriorityGroupView {
    val name: String
    val requestQueue: List<PrioritizedRunnable>
}

class PriorityGroup(override val name: String): PriorityGroupView {
    private val _requestQueue = LinkedList<PrioritizedRunnable>()
    override val requestQueue: List<PrioritizedRunnable> get() = _requestQueue

    val isEmpty: Boolean get() = _requestQueue.isEmpty()

    fun offer(runnable: PrioritizedRunnable) {
        _requestQueue.add(runnable)
    }

    fun poll(): PrioritizedRunnable? = _requestQueue.pollFirst()

    fun extractIf(filter: (PrioritizedRunnable) -> Boolean): RichIterable<PrioritizedRunnable> {
        return _requestQueue.extractIf(filter)
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
