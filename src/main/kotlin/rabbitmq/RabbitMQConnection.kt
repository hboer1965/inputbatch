package nl.tkp.ipde.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CountDownLatch

class RabbitMQConnection {

    private val connection: Connection
//    private val queueCleaner: QueueCleaner

    constructor() {
        val connectionFactory = ConnectionFactory()
//        connectionFactory.host = "rabbitmq"
        connectionFactory.host = "localhost"
        connectionFactory.port = 5672
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"
        connection = connectionFactory.newConnection()
//        queueCleaner = QueueCleaner(connection)
//        Thread(queueCleaner).start()
    }

//    fun registerObsoleteQueue(name: String) {
//        queueCleaner.registerObsoleteQueue(name)
//    }

    fun createChannel(): Channel {
        logger.info("Creating a new channel")
        return connection.createChannel()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RabbitMQConnection::class.java)
    }

    fun close() {
//        queueCleaner.stop()
        connection.close()
    }

    class QueueCleaner(connection: Connection) : Runnable {

        private val queuesToDelete = Collections.synchronizedSet(mutableSetOf<String>())
        private val channel = connection.createChannel()
        private val doneLatch = CountDownLatch(1)

        override fun run() {
            log.info("Start van de queueCleaner")
            while (doneLatch.count != 0L) {
                Thread.sleep(1000)
                log.info("Number of items in queuesToDelelet ${queuesToDelete.size}")
                queuesToDelete.forEach {
                    channel.queueDelete(it)
                }
                log.info("Number of items in queuesToDelelet ${queuesToDelete.size}")
            }
            log.info("Stopping queueCleaner")
        }

        fun stop() {
            doneLatch.countDown()
        }

        companion object {
            val log: Logger = LoggerFactory.getLogger(QueueCleaner::class.java)
        }

        fun registerObsoleteQueue(name: String) {
            log.info("Adding a queue $name")
            queuesToDelete.add(name)
            log.info("Adding a queue ${queuesToDelete.size}")
        }
    }
}