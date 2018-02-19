package inputbatch

import commons.BrokerConnection
import commons.JobVragenConnection
import java.sql.Timestamp
import java.util.concurrent.atomic.AtomicBoolean

//import org.slf4j.LoggerFactory

val terminationRequested = AtomicBoolean(false);

class Application {
//    private val logger = LoggerFactory.getLogger(Application::class.java)!!
//    private val config = Config(null)

    private val jobVragenConnection = JobVragenConnection("jdbc:oracle:thin:@localhost:1521/XE", "system", "oracle")
    private val brokerConnection = BrokerConnection("localhost", "admin", "rabbitmq_pass")

    private val maxTriesPerJob = 0

    fun start(channel: String) {
        println("Starting application input-batch reading jobs from ${channel}...")

        while (!terminationRequested.get()) {
            val questionsCount = getAndPostJobQuestions()

            if (questionsCount == 0)  Thread.sleep(10000) // sleep some time for avoiding database stress
        }
    }

    data class IdDatetime(val id: Long, val dateTime: Timestamp?)
    fun getAndPostJobQuestions(): Int {
        val job = jobVragenConnection.getAndLockJob(maxTriesPerJob)
        if (job == null) return 0

        val jobQuestions = jobVragenConnection.selectJobVragen(job.jobId)

        val vragenPerRegistratiedatumtijd = jobQuestions.groupBy { it.registratie_datumtijd }
        val x = vragenPerRegistratiedatumtijd.map {k -> k.key}
//scala
//        val ipdeResults = vragenPerRegistratiedatumtijd.map {
//            case (regdate, vragen) => {


        if (jobQuestions.size > 0) {
            try {
                brokerConnection.postQuestions(jobQuestions, job.priority)
                brokerConnection.postJobReadyCheck(job.jobId)
                jobVragenConnection.updateJobToPosted(job.jobId)
                println("Posted ${jobQuestions.size} vragen of job ${job.jobId} to IPDE.")
            } catch (e: Exception) {
                jobVragenConnection.rollback() // rollback the locked job!
                throw e
            }
        } else {
            println("Job with id=${job.jobId} was selected for processing, but has no remaining questions.")
            jobVragenConnection.commit() // release locked job
        }
        return jobQuestions.size
    }

}

fun main(args: Array<String>) {

//    Signal.handle(Signal("INT"), object : SignalHandler {
//        override fun handle(sig: Signal) {
//            terminationRequested.set(true)
//        }
//    })

    // setup application
    Application().start("ukd")
}