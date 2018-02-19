package JobGuardian

import commons.BrokerConnection
import commons.JobAdmin
import commons.JobVragenConnection
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.launch


//import org.slf4j.LoggerFactory

class Application {
//    private val logger = LoggerFactory.getLogger(Application::class.java)!!
//    private val config = Config(null)

    private val jobVragenConnection = JobVragenConnection("jdbc:oracle:thin:@localhost:1521/XE", "system", "oracle")
    private val consumeConn = BrokerConnection("localhost", "admin", "rabbitmq_pass")
    private val publishConn = BrokerConnection("localhost", "admin", "rabbitmq_pass")

    fun start(input: String) {
        println("Starting application input-batch guarding jobs from ${input}...")

        reprocessJobsInProgress()

        val channel = Channel<JobAdmin>()
        launch { consumeConn.StartSendingReadyChecks(channel).start() }

        launch {
            for (job in channel) {
                if (jobVragenConnection.jobHasQuestionsUnanswered(job.jobId)) {
                    val timeRunning = System.currentTimeMillis() - job.startTimeInMilliseconds
                    if (timeRunning > 3600_000) {
                        println("Job with id=${job.jobId} is retried again (${timeRunning} > 3600_000).")
                        jobVragenConnection.postJobToVerwerken(job.jobId)
                    } else {
                        publishConn.postJobReadyCheck(job.jobId, job.readyCheckInMilliseconds, job.startTimeInMilliseconds)
                    }
                }
            }
        }
    }

    fun reprocessJobsInProgress() {
        val jobsInProgress = jobVragenConnection.selectJobVsInProgress()
        jobsInProgress.forEach {
            publishConn.postJobReadyCheck(it.jobId, 10000, it.starttime.time)
        }
        println("Startup: posted ${jobsInProgress.size} job(s) for retrying.")
    }
}

fun main(args: Array<String>) {
    Application().start("ukd")
}