package JobGuardian

import org.slf4j.LoggerFactory
import java.sql.DriverManager
import kotlin.system.measureTimeMillis

val applicationDatabaseLockSql =
        """
declare
     l_handle_name varchar2(128);
     l_lockhandle VARCHAR2(128);
     l_res number;
begin
     l_handle_name := 'ipde-JobGuardian-' || '?';
     dbms_lock.allocate_unique(l_handle_name, l_lockhandle);
     ? := dbms_lock.request(lockhandle => l_lockhandle
       , lockmode => dbms_lock.x_mode
       , timeout => 0
       , release_on_commit => FALSE);
end;
"""

val jobsBackToVerwerkenSql = """
update pas_jobs
set ind_verwerken = 'J'
where ind_verwerken = 'I'
"""

val jobsToDoneSql = """
update pas_jobs j
set j.ind_verwerken = 'N'
,   j.einddatum = sysdate
where  j.ind_verwerken = 'I'
and    not exists
       ( select 1
         from   pas_formulariumvragen v
         where  v.job_id = j.id
         and    not exists ( select 1
                             from   pas_fmg_fouten f
                             where  f.fmg_id = v.id
                           )
         and    not exists ( select 1
                             from   pas_resultaten r
                             where  r.rst_id = v.id
                           )
       )
"""

class JobGuardianConnection(address: String, user: String, pass: String) {
    private val connection = DriverManager.getConnection(address, user, pass)
    private val LockStmt = connection.prepareCall(applicationDatabaseLockSql)
    private val updateBackToVerwerken = connection.prepareStatement(jobsBackToVerwerkenSql)
    private val updateToDone = connection.prepareStatement(jobsToDoneSql)


    init {
        connection.setAutoCommit(true)
    }

    fun acquireLock(lockSuffix: String): Boolean {
        var lockResultCode: Int = -1;
        LockStmt.setString(1, lockSuffix)
        LockStmt.setInt(2, lockResultCode)
        LockStmt.execute()

        return (lockResultCode == 0)
    }


    fun updateJobsBackToVerwerken(): Int {
        return updateBackToVerwerken.executeUpdate()
    }

    fun updateJobsToDone(): Int {
        return updateToDone.executeUpdate()
    }

    fun close() = connection.close()
}

class Application(private val queueNameSuffix: String) {

    private val connection = JobGuardianConnection("jdbc:oracle:thin:@localhost:1521/XE", "system", "oracle")

    fun start() {
        getExclusiveDatabaseLock()

        println("Start van de message loop")

        var jobsCount = 0
        var duration = measureTimeMillis {
            jobsCount = connection.updateJobsBackToVerwerken()
        }
        logger.info("Initialize Job-guardian, updated ${jobsCount} job(s) back to verwerken in ${duration} milliseconds.")

        while (true) {
            duration = measureTimeMillis {
                jobsCount = connection.updateJobsToDone()
                logger.info("Job-guardian, updated ${jobsCount} job(s) to gereed in ${duration} milliseconds.")
            }
            Thread.sleep(if (jobsCount == 0) 15_000 else 2_000)
        }
    }

    fun getExclusiveDatabaseLock() {
        if (!connection.acquireLock(queueNameSuffix)) {
            logger.error("Cannot acquire application lock in database, exitting.")
            println("Cannot acquire application lock in database, exitting.")
            connection.close()
            System.exit(1)
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(Application::class.java)
    }
}

fun main(args: Array<String>)  {

    // TODO - Tests

    val argParser = ArgParser(args)
    val applicationName by argParser.storing("--application-name", help = "Naam van de applicatie").default("input-batch")
    val queueNameSuffix by argParser.storing("--queue-name-suffix", help = "Suffix voor queue naam ipde.results.").default("input-batch")
    val topologyFile by argParser.storing("--topology", help = "filename and location of the topology.cfg").default("topology.cfg")

    IpdeApplication(applicationName, *args).use { ipdeApplication ->
        val topology = Topology(topologyFile)
        val parameters = Parameters.constructFromTopologyAndConfig(topology, applicationName)
        println("Using database connection ${parameters.databaseConnection}, all parameters $parameters")

        Runtime.getRuntime().addShutdownHook(Thread {
            System.err.println("Shutting down ${applicationName}")
            ipdeApplication.close()
            System.err.println("DONE Shutting down ${applicationName}")
        })

        Application(queueNameSuffix).start()

        println("Start van de message loop")

        ipdeApplication.await()
    }
}




/*
package nl.tkp.ipde.batch

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import ipde.model.Data
import ipde.model.antwoord.Antwoord
import ipde.model.vraag.Vraag
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import nl.tkp.ipde.batch.repositories.Database
import nl.tkp.ipde.batch.repositories.ResultRepository
import nl.tkp.ipde.commons.IpdeApplication
import nl.tkp.ipde.commons.Topology
import nl.tkp.ipde.communication.LogicalQueueName
import nl.tkp.ipde.communication.MetricClient
import nl.tkp.ipde.communication.QueueMessage
import org.jetbrains.exposed.sql.transactions.transaction
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


val aantalGeenAfname = AtomicInteger(0)
val aantalFouten = AtomicInteger(0)
val aantalResultaten = AtomicInteger(0)


class Writer(private val resultRepository: ResultRepository) {

    fun checkAndSaveToDatabase(message: QueueMessage) {
        try {
            val jobId = message.headers!!["jobId"].toString().toInt()
            val bdeId = message.headers!!["bdeId"].toString().toInt()
            val fmgId = message.headers!!["fmgId"].toString().toInt()
            val flexRekendatum = message.headers!!["flexRekendatum"].toString()
println("Got something, fmgId=${fmgId}; bdeId=${bdeId}; jobId=${jobId}")
            val data = Data.MessageData.parseFrom(message.body)

            if (data.vraag.vraagsoort != Vraag.VraagSoort.LEGACY_DVD) {
                logger.error("Alleen schrijven van LEGACY_DVD antwoorden wordt ondersteund, ontvangen=${data.vraag.vraagsoort}")
            } else {
                resultRepository.saveToDatabase(data, fmgId, bdeId, jobId, flexRekendatum)
            }
        } catch (e: Exception) { // TODO more finegrained
            logger.error("Onverwachte exceptie bij schrijven: ${e.message}")
println("Onverwachte exceptie bij schrijven: ${e.message}")
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(Writer::class.java)
    }
}

suspend fun writeMetrics(metricClient: MetricClient) {
    while (true) {
        delay(60, TimeUnit.SECONDS)

        val aantalResultaten = aantalResultaten.getAndSet(0)
        val aantalFouten     = aantalFouten.getAndSet(0)
        val aantalGeenAfname = aantalGeenAfname.getAndSet(0)

        if (aantalResultaten > 0) metricClient.sendCountMetricWithAantal(aantalResultaten, "resultaten.count", "")
        if (aantalFouten > 0)     metricClient.sendCountMetricWithAantal(aantalFouten, "fouten.count", "")
        if (aantalGeenAfname > 0) metricClient.sendCountMetricWithAantal(aantalGeenAfname, "geenafname.count", "")
    }

}

fun main(args: Array<String>) = runBlocking {

    // TODO - Tests

    val argParser = ArgParser(args)
    val applicationName by argParser.storing("--application-name", help = "Naam van de applicatie").default("input-batch")
    val queueNameSuffix by argParser.storing("--queue-name-suffix", help = "Suffix voor queue naam ipde.results.").default("input-batch")
    val topologyFile by argParser.storing("--topology", help = "filename and location of the topology.cfg").default("topology.cfg")

    IpdeApplication(applicationName, *args).use { ipdeApplication ->
        val topology = Topology(topologyFile)
        val parameters = Parameters.constructFromTopologyAndConfig(topology, applicationName)
        println("Using database connection ${parameters.databaseConnection}, all parameters $parameters")

        Database.openOracleConnection(parameters.databaseConnection)
        val ipdeQueueConnection = ipdeApplication.ipdeQueueConnection
        ipdeQueueConnection.declareResultQueue("ipde.results.${queueNameSuffix}")
        val writer = Writer(ResultRepository())

        launch { writeMetrics(ipdeApplication.metricSender) }

        Runtime.getRuntime().addShutdownHook(Thread {
            System.err.println("Shutting down inputbatch-writer")
            ipdeQueueConnection.close()
            ipdeApplication.close()
            System.err.println("DONE Shutting down inputbatch-writer")
        })

        println("Start van de message loop")
        ipdeQueueConnection.startConsumerOn(LogicalQueueName.INPUTBATCH_RESULT, queueNameSuffix)
        {message ->
            writer.checkAndSaveToDatabase(message)
            message.ack()
        }

        ipdeApplication.await()
    }
}

 */