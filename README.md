### Running
```kotlin 
val jobId = WorkerManager.enqueue {
  // do work withn WorkerContext
  ...
  // access to jobs progress flow via context
  update(0.2f)
}

//observe running and queued
WorkerManager.running.collect()
```
### Completed
```kotlin
WorkerManager.completed.collect { completedJobs -> ... } 
```
### Cancel 
```kotlin
// cancel work by UUID
WorkerManager.cancel(jobId)
```
### Retry
```kotlin
// automatic retrying
  val job = WorkerManager.enqueue(
      req = Req(
          maxRetry = 3,
          work = { // within context access to retry count
              print("retry $retry")
          }
      )
  ) 
```

