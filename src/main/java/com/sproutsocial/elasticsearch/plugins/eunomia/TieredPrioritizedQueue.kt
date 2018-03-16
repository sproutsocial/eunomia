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

class TieredPrioritizedQueue(activeRunables: ListIterable<PrioritizedRunnable>) : TieredPrioritizedQueueView {
    var deadlineInNanos: Long = TimeUnit.SECONDS.toNanos(10)
    var reprioritizeIntervalInNanos: Long = TimeUnit.SECONDS.toNanos(1)

    private var nextReprioritizeTimestampNano: Long = 0

    private val _tiers: ListIterable<PriorityTier> = Lists.immutable.of(
            PriorityTier("very high", activeRunables),
            PriorityTier("high", activeRunables),
            PriorityTier("normal", activeRunables),
            PriorityTier("low", activeRunables),
            PriorityTier("very low", activeRunables))

    @Suppress("UNCHECKED_CAST")
    override val tiers: ListIterable<PriorityTierView> get() = _tiers as ListIterable<PriorityTierView>


    fun offer(prioritizedRunnable: PrioritizedRunnable) {
        val targetTier = calculateTierIndex(prioritizedRunnable)
        _tiers[targetTier].offer(prioritizedRunnable)
    }

    private fun calculateTierIndex(prioritizedRunnable: PrioritizedRunnable): Int =
            when (prioritizedRunnable.inFlight) {
                true -> 0
                else -> calculateTierIndexFromTimestamp(prioritizedRunnable, System.nanoTime())
            }

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

class PriorityTier(override val name: String,
                   private val activeRunables: ListIterable<PrioritizedRunnable>) : PriorityTierView {
    private val _priorityGroups = Lists.mutable.empty<PriorityGroup>()

    @Suppress("UNCHECKED_CAST")
    override val priorityGroups: ListIterable<PriorityGroupView> get() = _priorityGroups as ListIterable<PriorityGroupView>

    private var nextGroupIndex = 0
    private var shouldOptimize = false

    private val random = Random()

    fun offer(runnable: PrioritizedRunnable) {
        findOrCreatePriorityGroup(runnable.priorityGroup).offer(runnable)
    }

    fun poll(): PrioritizedRunnable? {
        optimize()
        if (priorityGroups.isEmpty) { return null }
        if (priorityGroups.size() == 1) { return _priorityGroups[0].poll() }

        val activeGroups = activeRunables.aggregateBy({it.priorityGroup}, {0}, {v, _ -> v + 1})
        val pendingGroupUsage = _priorityGroups.collect { Pair(it, activeGroups.getIfAbsent(it.name, {0})) }
        val minUsage = pendingGroupUsage.collectInt { it.second }.min()
        val candidateGroups = pendingGroupUsage.collectIf({ it.second == minUsage }, {it.first})

        return candidateGroups[random.nextInt(candidateGroups.size)].poll()
    }

    private fun optimize() {
        if (_priorityGroups.isEmpty) { return }
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
