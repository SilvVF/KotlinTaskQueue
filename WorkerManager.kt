import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.filterNot
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

data class Req(
    val context: CoroutineContext = Dispatchers.Default,
    val maxRetry: Int = 3,
    val work: Worker,
)

data class WorkJob(
    val id: UUID = UUID.randomUUID(),
    val req: Req,
    internal val _progress: MutableStateFlow<Float> = MutableStateFlow(0.0f),
    internal val _state: MutableStateFlow<JobState> = MutableStateFlow(Idle),
    val retry: Int = 0
) {

    val state: JobState
        get() = _state.value

    val progress: Float
        get() = _progress.value

    enum class JobState {
        Idle,
        Queued,
        Completed,
        Working,
        Failed,
        Cancelled
    }
}

interface WorkContext {
    val retry: Int
    fun update(value: Float)

    companion object {
        fun createCtx(job: WorkJob): WorkContext {
            return  object : WorkContext {
                override val retry: Int
                    get() = job.retry

                override fun update(value: Float) {
                    job._progress.update { value }
                }
            }
        }
    }
}

fun interface Worker {

    context(WorkContext)
    suspend fun doWork()
}

object WorkerManager {

    private val mutex = Mutex()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val activeJobs = ConcurrentHashMap<UUID, Job>()

    private val queue = MutableStateFlow<List<WorkJob>>(emptyList())
    val completed = MutableStateFlow<List<WorkJob>>(emptyList())

    val running = queue.map { jobs ->
        jobs.filter { it.state == Working || it.state == Queued }
    }
        .stateIn(
            scope,
            SharingStarted.Lazily,
            emptyList()
        )

    fun enqueue(worker: Worker): UUID {
        val job = WorkJob(req = Req(work = worker))
        queue.update { jobs ->
            buildList {
                addAll(jobs)
                add(job)
            }
        }
        return job.id
    }

    fun enqueue(req: Req): UUID {
        val job = WorkJob(req = req)
        queue.update { jobs ->
            buildList {
                addAll(jobs)
                add(job)
            }
        }
        return job.id
    }

    fun cancel(id: UUID) {
        val job = queue.value.find { it.id == id }
        if (job != null) {
            activeJobs[job.id]?.cancel()
            job._state.value = Cancelled
        }
    }

    init {
        queue.asStateFlow()
            .filterNot { it.isEmpty() }
            .onEach { jobs ->
                val queued = mutex.withLock {
                   jobs
                        .filter { job -> job.retry <= job.req.maxRetry }
                        .filter { job -> job.state == Idle || job.state == Failed }
                        .onEach { job ->
                            job._state.value = Queued
                        }
                }

                supervisorScope {
                    for (job in queued) {

                        if (job._state.value == Cancelled) continue

                        job._state.value = Working
                        job._progress.value = 0.0f

                        activeJobs[job.id] = launch(job.req.context) {
                            val res = runCatching {
                                with(WorkContext.createCtx(job)) {
                                    job.req.work.doWork()
                                }
                            }
                            job._state.value = res.fold(
                                onFailure = { t ->
                                    when(t) {
                                        is CancellationException ->  Cancelled
                                        else -> Failed
                                    }
                                },
                                onSuccess = { Completed }
                            )
                            activeJobs.remove(job.id)
                        }

                        activeJobs[job.id]?.join()
                    }
                }
                val requeue = jobs
                    .filterNot { job -> job.state == Completed || job.state == Cancelled }
                    .map {
                        it.copy(
                            retry = it.retry + 1,
                            id = it.id,
                            req = it.req,
                            _progress = it._progress,
                            _state = it._state,
                        )
                    }

                completed.update { complete ->
                    complete + jobs.filter { job -> job.state == Completed || job.state == Cancelled }
                }

               queue.update { requeue }
            }
            .launchIn(scope)
    }
}
