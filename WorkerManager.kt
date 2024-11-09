package ios.silv.myapplication.ui

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.transformLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlin.coroutines.cancellation.CancellationException

interface Notifier {
    fun onWarning(reason: String?)
    fun onError(message: String?)
    fun onComplete()
    fun onPaused()
    fun dismissProgress()

    companion object {
        val NoOpNotifier = object: Notifier {
            override fun dismissProgress() = Unit
            override fun onWarning(reason: String?) = Unit
            override fun onError(message: String?) = Unit
            override fun onComplete() = Unit
            override fun onPaused() = Unit
        }
    }
}


@ExperimentalCoroutinesApi
class SusQueue <T, K> (
    val keySelector: (T) -> K,
    val doWork: suspend (T) -> Result<Unit>,
    private val parallel: Int = 3,
    private val workScope: CoroutineScope = CoroutineScope(Dispatchers.IO),
    private val notifier: Notifier = Notifier.NoOpNotifier,
    private val initialItems: List<T> = emptyList()
): Notifier by notifier {

    data class Item<T>(
        val data: T,
        val retryCount: Int
    ) {
        @Transient
        private val _statusFlow = MutableStateFlow(State.IDLE)

        @Transient
        val statusFlow = _statusFlow.asStateFlow()
        var status: State
            get() = _statusFlow.value
            set(status) {
                _statusFlow.value = status
            }


        enum class State(val value: Int) {
            IDLE(0),
            QUEUE(1),
            RUNNING(2),
            COMPLETED(3),
            ERROR(4),
        }
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    init {
        scope.launch {
            addAllToQueue(initialItems, 0)
        }
    }

    private val _queueState = MutableStateFlow<List<Item<T>>>(emptyList())
    val queueState = _queueState.asStateFlow()

    private var queueJob: Job? = null

    val isRunning: Boolean
        get() = queueJob?.isActive ?: false


    private fun areAllJobsFinished(): Boolean {
        return queueState.value.none { it.status.value <= Item.State.RUNNING.value }
    }


    private fun cancelQueueJob() {
        queueJob?.cancel()
        queueJob = null
    }

    fun stop(reason: String? = null) {
        cancelQueueJob()
        queueState.value
            .filter { it.status == Item.State.RUNNING }
            .forEach { it.status = Item.State.ERROR }

        if (reason != null) {
            onWarning(reason)
            return
        }

        if (queueState.value.isNotEmpty()) {
            onPaused()
        } else {
            onComplete()
        }
    }


    fun pause() {
        cancelQueueJob()
        queueState.value
            .filter { it.status == Item.State.RUNNING }
            .forEach { it.status = Item.State.QUEUE }
    }

    private fun internalClearQueue() {
        _queueState.update {
            it.forEach { item ->
                if (item.status == Item.State.RUNNING ||
                    item.status == Item.State.QUEUE
                ) {
                    item.status = Item.State.IDLE
                }
            }
            emptyList()
        }
    }

    fun clearQueue() {
        cancelQueueJob()
        internalClearQueue()
        dismissProgress()
    }

    private fun addAllToQueue(items: List<T>, retryCount: Int) {
        _queueState.update {
            val queueItems = items.map { item ->
                Item(data = item, retryCount).apply {
                    status = Item.State.QUEUE
                }
            }
            it + queueItems
        }
    }

    private fun removeFromQueueIf(predicate: (T) -> Boolean) {
        _queueState.update { queue ->
            val items = queue.filter { predicate(it.data) }
            items.forEach { item ->
                if (item.status == Item.State.RUNNING ||
                    item.status == Item.State.QUEUE
                ) {
                    item.status = Item.State.COMPLETED
                }
            }
            queue - items.toSet()
        }
    }

    private fun areAllItemsFinished(): Boolean {
        return queueState.value.none { it.status.value <= Item.State.RUNNING.value }
    }

    fun start(): Boolean {
        if (isRunning || queueState.value.isEmpty()) {
            return false
        }

        val pending = queueState.value.filter { it.status != Item.State.COMPLETED }
        pending.forEach { if (it.status != Item.State.QUEUE) it.status = Item.State.QUEUE }

        launchQueueJob()

        return pending.isNotEmpty()
    }

    private fun removeFromQueue(item: Item<T>) {
        _queueState.update {
            if (item.status == Item.State.RUNNING || item.status == Item.State.QUEUE) {
                item.status = Item.State.COMPLETED
            }
            it - item
        }
    }

    private suspend fun <T> retry(retries: Int, predicate: suspend (attempt: Int) -> Result<T>): Result<T> {
        require(retries > 0) { "Expected positive amount of retries, but had $retries" }
        var throwable: Throwable? = null
        (1..retries).forEach { attempt ->
            try {
                val result = predicate(attempt)
                if (result.isSuccess) {
                    return result
                }
                throwable = result.exceptionOrNull()
            } catch (e: Throwable) {
                throwable = e
            }
        }
        return Result.failure(throwable ?: IllegalStateException())
    }

    private fun launchQueueJob() {
        if (isRunning) return

        queueJob = scope.launch {
            val activeJobsFlow = queueState.transformLatest { queue ->
                while (true) {
                    val activeJobs = queue.asSequence()
                        .filter {
                            it.status.value <= Item.State.RUNNING.value
                        } // Ignore completed items, leave them in the queue
                        .groupBy(keySelector = { keySelector(it.data) })
                        .toList()
                        .take(parallel)
                        .map { (_, items) -> items.first() }
                    emit(activeJobs)

                    if (activeJobs.isEmpty()) break

                    // Suspend until an item enters the ERROR state
                    val activeJobsErroredFlow =
                        combine(activeJobs.map(Item<T>::statusFlow)) { states ->
                            states.contains(Item.State.ERROR)
                        }.filter { it }
                    activeJobsErroredFlow.first()
                }

                if (areAllJobsFinished()) stop()
            }.distinctUntilChanged()

            supervisorScope {
                val queueJobs = mutableMapOf<Item<T>, Job>()

                activeJobsFlow.collectLatest { activeJobs ->
                    val jobsToStop = queueJobs.filter { it. key !in activeJobs }
                    jobsToStop.forEach { (item, job) ->
                        job.cancel()
                        queueJobs.remove(item)
                    }

                    val itemsToStart = activeJobs.filter { it !in queueJobs }
                    itemsToStart.forEach { item ->
                        queueJobs[item] = workScope.launch {
                            try {
                                val result = retry(item.retryCount + 1) {
                                    doWork(item.data)
                                }
                                if (result.isSuccess) {
                                    item.status = Item.State.COMPLETED
                                    removeFromQueue(item)
                                } else {
                                    item.status = Item.State.ERROR
                                }
                            } catch (e: Throwable) {
                                if (e is CancellationException) {
                                    onError("Item cancelled")
                                } else {
                                    notifier.onError(e.message)
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
