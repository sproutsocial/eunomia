package com.sproutsocial.elasticsearch.plugins.eunomia

interface PrioritizedRunnable {
    val action: String
    val isForceExecuted: Boolean
    val priority: Int
    val priorityGroup: String
    val inFlight: Boolean
    val receivedTimestampInNanos: Long
    val executor: String

    fun run(onComplete: () -> Unit)
}