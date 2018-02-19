package commons

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import kotlinx.coroutines.experimental.launch
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

data class JobAdmin(val jobId: Long, val startTimeInMilliseconds:Long, val readyCheckInMilliseconds: Long)

data class Results(val iets: Long)

class BrokerConnection(host: String, user: String, pass: String) {

    private val jobExchangeName = "job-admin-exchange"
    private val jobReadyQueueName = "job-admin-queue"
    private val vraagQueueName = "ipde-in"
    private val resultsQueueName = "ipde-out"
    private val EMPTYQUEUENAME = ""
    private val EMPTYEXCHANGENAME = ""
    private val ReadyCheckRoutingKey = "ready-check"

    private val rmqConnection = getRmqConnection(host, user, pass)
    private val vraagChannel = getVraagChannel()
    private val resultsChannel = getResultsChannel()
    private val jobPublishChannel = getJobPublishChannel()
    private val jobReadyConsumeChannel = getJobReadyConsumeChannel()

    private val latch = CountDownLatch(1)

    fun postQuestions(jobQuestions: List<VraagData>, priority: Int = 0) {
        val props = AMQP.BasicProperties.Builder().priority(10 - priority).build()
        jobQuestions.forEach {
            vraagChannel.basicPublish(EMPTYEXCHANGENAME, vraagQueueName,  props, it.toString().toByteArray(charset("UTF-8")))
        }
    }

    fun postJobReadyCheck(jobId: Long, readyCheckInMilliseconds: Long = 10000, startTimeInMilliseconds: Long = System.currentTimeMillis()) {
        val messageBodyBytes = jobId.toString().toByteArray(charset("UTF-8"))
        val headers = mapOf<String, Any>("x-delay" to readyCheckInMilliseconds, "starttime" to startTimeInMilliseconds)
        val props = AMQP.BasicProperties.Builder().headers(headers)
        println("Going to check ready Job with id=${jobId} in ${readyCheckInMilliseconds} milliseconds.")
        jobPublishChannel.basicPublish(jobExchangeName, ReadyCheckRoutingKey, props.build(), messageBodyBytes)
    }

    inner class StartSendingReadyChecks(private val sendChannel: kotlinx.coroutines.experimental.channels.Channel<JobAdmin>) {
        fun start() {
            val channel = jobReadyConsumeChannel
            channel.basicConsume(jobReadyQueueName, true,
                    RabbitMQConsumer(channel, {
                        val jobId = it.body.toString(charset("UTF-8")).toLong()
                        val headers = it.properties?.headers
                        val lastWaitTime = headers?.get("x-delay").toString().toLong()
                        val startTime = headers?.get("starttime").toString().toLong()
                        launch { sendChannel.send(JobAdmin(jobId, startTime, lastWaitTime)) }
                    })
            )
            latch.await()
            channel.close()
        }
    }

    inner class ConsumingResults(private val sendChannel: kotlinx.coroutines.experimental.channels.Channel<Results>) {
        fun start() {
            val channel = resultsChannel
            channel.basicConsume(resultsQueueName, true,
                    RabbitMQConsumer(channel, {
                        val jobId = it.body.toString(charset("UTF-8")).toLong()
                        val headers = it.properties?.headers
                        val lastWaitTime = headers?.get("x-delay").toString().toLong()
                        val startTime = headers?.get("starttime").toString().toLong()
                        launch { sendChannel.send(Results(1)) }
                    })
            )
            latch.await()
            channel.close()
        }
    }

    private fun abc() {
        val x = CompletableFuture<String>()
        x.get(5, TimeUnit.SECONDS)
    }

    private fun getRmqConnection(host: String, user: String, pass: String): Connection {
        val connectionFactory = com.rabbitmq.client.ConnectionFactory()
        connectionFactory.host = host
        connectionFactory.username = user
        connectionFactory.password = pass
        val conn = connectionFactory.newConnection()
        return conn
    }

    private fun getVraagChannel(): Channel {
        val channel = rmqConnection.createChannel()
        channel.queueDeclare(vraagQueueName , false, false, false, mapOf<String, Any>("x-max-priority" to 10))
        return channel
    }

    private fun getResultsChannel(): Channel {
        val channel = rmqConnection.createChannel()
        channel.queueDeclare(resultsQueueName , false, false, false, null)
        return channel
    }

    private fun getJobPublishChannel(): Channel {
        val channel = rmqConnection.createChannel()
        channel.queueDeclare(jobReadyQueueName , true, false, false, null)
        return channel
    }

    private fun getJobReadyConsumeChannel(): Channel {
        val channel = rmqConnection.createChannel()
        channel.exchangeDeclare(jobExchangeName, "x-delayed-message", true, false, mapOf<String, Any>("x-delayed-type" to "direct"))
        channel.queueDeclare(jobReadyQueueName , true, false, false, null)
        channel.queueBind(jobReadyQueueName, jobExchangeName, ReadyCheckRoutingKey)
        return channel
    }
}
