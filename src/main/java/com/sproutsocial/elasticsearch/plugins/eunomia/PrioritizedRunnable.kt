package com.sproutsocial.elasticsearch.plugins.eunomia

interface PrioritizedRunnable {
    val priority: Int
    val priorityGroup: String
    val inFlight: Boolean
    val receivedTimestampInNanos: Long
    val executor: String

    fun run(onComplete: () -> Unit)
}