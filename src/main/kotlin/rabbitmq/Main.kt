package nl.tkp.ipde.rabbitmq

import com.rabbitmq.client.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.charset.Charset
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Phaser
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


class RabbitMQRequestReply(private val channel: Channel, logicalTarget: String) {


    class RequestReplyConsumer(private val responseQueue: Queue<RequestReplyResponseItem>, channel: Channel) :  DefaultConsumer(channel) {
        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            log.info("Received a delivery from rekenaar with correlation id ${properties!!.correlationId}")
            responseQueue.offer(RequestReplyResponseItem(properties.correlationId, body!!))
        }
    }

    private val exchangeName = RabbitMQDictionary.logicalTargetToAMQExchangeName[logicalTarget]
    private val publishQueueName = RabbitMQDictionary.logicalTargetToAMQQueueName[logicalTarget]
    private val replyQueueName = channel.queueDeclare().queue
    data class RequestReplyResponseItem(val correlationId: String, val body: ByteArray)
    private val responseQueue = LinkedBlockingQueue<RequestReplyResponseItem>()

    init {
        channel.basicConsume(replyQueueName, true, RequestReplyConsumer(responseQueue, channel))
    }

    fun requestReply(body: ByteArray) : ByteArray {
        val correlationId = UUID.randomUUID().toString()
        val messageProperties = AMQP.BasicProperties.Builder().
                correlationId(correlationId).
                replyTo(replyQueueName).
                build()
        channel.basicPublish(exchangeName, publishQueueName, messageProperties, body)
        var requestReplyResponseItem: RequestReplyResponseItem? = null
        var retries = 0
        while (requestReplyResponseItem == null) {
            requestReplyResponseItem = responseQueue.poll(1, TimeUnit.SECONDS)
            retries ++
            if (requestReplyResponseItem != null && requestReplyResponseItem.correlationId != correlationId) {
                requestReplyResponseItem = null
            }
            if (retries > 5) {
                throw RuntimeException()
            }
        }
        return requestReplyResponseItem.body
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(RabbitMQRequestReply::class.java)
    }
}

class RabbitMQMultiRequestReply(private val channel: Channel, logicalTarget: String)  {

    private val exchangeName = RabbitMQDictionary.logicalTargetToAMQExchangeName[logicalTarget]
    private val publishQueueName = RabbitMQDictionary.logicalTargetToAMQQueueName[logicalTarget]
    val replyQueueName = channel.queueDeclare().queue

    fun requestReply(body: List<String>, consumer: Consumer) {
        val correlationId = UUID.randomUUID().toString()
        val messageProperties = AMQP.BasicProperties.Builder().
                correlationId(correlationId).
                headers(mapOf("expectedNumberOfMessages" to body.size)).
                replyTo(replyQueueName).
                build()
        log.info("Starting consumer on queue $replyQueueName for receiving messages ${body.size}")
        channel.basicConsume(replyQueueName, true, consumer)
        body.forEach {
            log.info("Sending message with correlationId $correlationId $it")
            channel.basicPublish(exchangeName, publishQueueName, messageProperties, it.toByteArray(Charsets.UTF_8))
        }
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(RabbitMQRequestReply::class.java)
    }
}

class Dispenser(private val connection: RabbitMQConnection, private val dispenserId: Int) {

    fun start() {
        log.info("Starting dispenser")
        val messages = listOf("message1-$dispenserId", "message2-$dispenserId")
        val channel = connection.createChannel()
        val requestReplyQueue = RabbitMQRequestReply(channel, "rekenaar")

        messages.forEach {
            val result = requestReplyQueue.requestReply(it.toByteArray(Charsets.UTF_8)).toString(Charsets.UTF_8)
            log.info("Antwoord van rekenaar $result")
        }
        channel.close()
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(Dispenser::class.java)
        private var sequence = AtomicInteger(0)

        fun start(connection: RabbitMQConnection): Thread {
            val dispenserId = sequence.incrementAndGet()
            val dispenser = Dispenser(connection, dispenserId)
            val thread = Thread(Runnable {
                dispenser.start()
            }, "dispenser-thread-" + dispenserId)
            thread.start()
            return thread
        }
    }
}


class DeelnameDispenser(private val connection: RabbitMQConnection, private val dispenserId: Int) {

    val channel = connection.createChannel()
    val doneLatch = Phaser(0)

    class PurConsumer(private val connection: RabbitMQConnection, channel: Channel, private val doneLatch: Phaser) : DefaultConsumer(channel) {
        private val numberOfMessageReceived = AtomicInteger(0)

        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            val expectedNumberOfMessages = properties!!.headers["expectedNumberOfMessages"]
            val messageReceived = numberOfMessageReceived.incrementAndGet()
            if (messageReceived == expectedNumberOfMessages) {
                log.info("Done with pur")
                channel.basicCancel(this.consumerTag)
                doneLatch.arriveAndDeregister()
            }
        }
    }

    class BegunstigdeConsumer(private val connection: RabbitMQConnection, channel: Channel, private val purConsumer: PurConsumer) : DefaultConsumer(channel) {
        private val begunstigdeReplies = mutableListOf<String>()

        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            super.handleDelivery(consumerTag, envelope, properties, body)
            begunstigdeReplies.add(body!!.toString(Charsets.UTF_8))
            val expectedNumberOfMessages = properties!!.headers["expectedNumberOfMessages"]
            if (begunstigdeReplies.size == expectedNumberOfMessages) {
                log.info("Begunstigde is done, sending to rekenaar for PUR")
                val multiRequestReplyQueue = RabbitMQMultiRequestReply(channel, "rekenaar")
                multiRequestReplyQueue.requestReply(begunstigdeReplies, purConsumer)
                channel.basicCancel(this.consumerTag)
            }
        }
    }

    fun receiveMessages(messages: List<String>) {
        doneLatch.register()
        val multiRequestReplyQueue = RabbitMQMultiRequestReply(channel, "rekenaar")
        val purConsumer = PurConsumer(connection, channel, doneLatch)
        val begunstigdeConsumer = BegunstigdeConsumer(connection, channel, purConsumer)
        multiRequestReplyQueue.requestReply(messages, begunstigdeConsumer)
    }

    fun join() {
        log.info("Start join dispenser")
        doneLatch.awaitAdvance(0)
        channel.close()
    }

    companion object {
        val log: Logger = LoggerFactory.getLogger(DeelnameDispenser::class.java)
        private val sequence = AtomicInteger(0)

        fun start(connection: RabbitMQConnection): DeelnameDispenser {
            val dispenserId = sequence.incrementAndGet()
            return DeelnameDispenser(connection, dispenserId)
        }
    }
}

class Rekenaar(private val rekenaarId: Int, private val connection: RabbitMQConnection) {

    private val latch = CountDownLatch(1)


    class RekenaarConsumer(private val rekenaarId: Int, channel: Channel) : DefaultConsumer(channel) {

        override fun handleDelivery(consumerTag: String?, envelope: Envelope?, properties: AMQP.BasicProperties?, body: ByteArray?) {
            val correlationId = properties?.correlationId
            val replyTo = properties!!.replyTo
            log.info("Rekenaar $rekenaarId Handling message with correlation id $correlationId, replying to $replyTo")

            val replyProps = AMQP.BasicProperties.Builder().headers(properties.headers).correlationId(correlationId).build()

            val response = "Hallo van de rekenaar op message ${body?.toString(Charsets.UTF_8)}"
            channel.basicPublish("", replyTo, replyProps, response.toByteArray(Charset.forName("UTF-8")))
            channel.basicAck(envelope!!.deliveryTag, false)
        }
    }

    fun start() {
        val channel = connection.createChannel()
        val rabbitMQConsumer = RabbitMQConsumer(channel, "rekenaar")

        val rekenaarConsumer = RekenaarConsumer(rekenaarId, channel)
        rabbitMQConsumer.startConsumeWithConsumer(rekenaarConsumer)
        latch.await()
        channel.close()
    }

    fun stop() {
        latch.countDown()
    }

    companion object {
        private val log = LoggerFactory.getLogger(Rekenaar::class.java)
        private val sequence = AtomicInteger(0)

        fun start(connection: RabbitMQConnection): Rekenaar {
            val rekenaarId = sequence.incrementAndGet()
            val rekenaar = Rekenaar(rekenaarId, connection)
            Thread(Runnable {
                rekenaar.start()
            }, "rekenaar-thread-" + rekenaarId).start()
            return rekenaar
        }
    }
}


fun main(args: Array<String>) {
    println("Starting app")

    val rabbitConnection = RabbitMQConnection()

    val rekenaar1 = Rekenaar.start(rabbitConnection)
    val rekenaar2 = Rekenaar.start(rabbitConnection)

    // dispenser is een simpele 1 message naar rekenaar en 1 message weer retour en dispenser is klaar.
    val t1 = Dispenser.start(rabbitConnection)
    val t2 = Dispenser.start(rabbitConnection)
    val t3 = Dispenser.start(rabbitConnection)
    val t4 = Dispenser.start(rabbitConnection)
    t1.join()
    t2.join()
    t3.join()
    t4.join()

    // deelname dispenser is een x-aantal message naar
    // dispenser -> rekenaar -> begunstigde,
    // begunstigde -> rekenaar -> pur en klaar.
    val deelnameDispenser = DeelnameDispenser.start(rabbitConnection)

    deelnameDispenser.receiveMessages(listOf("message-1", "message-2")) // alle receives op main. Dit is potentieel een thrread pool
    deelnameDispenser.receiveMessages(listOf("message-2-1"))
    deelnameDispenser.receiveMessages(listOf("message-2-1"))
    deelnameDispenser.receiveMessages(listOf("message-2-1"))
    deelnameDispenser.receiveMessages(listOf("message-3-1"))
    deelnameDispenser.receiveMessages(listOf("message-4-1"))
    deelnameDispenser.receiveMessages(listOf("message-5-1", "message-5-2", "message-5-3", "message-5-4", "message-5-5", "message-5-6", "message-5-7", "message-5-8"))
    deelnameDispenser.join()

    rekenaar1.stop()
    rekenaar2.stop()
    rabbitConnection.close()
}

