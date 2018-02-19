import com.rabbitmq.client.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

object ConcurrentRecv2 {
    private val logger = LoggerFactory.getLogger(ConcurrentRecv2::class.java)

    private val QUEUE_NAME = "hello"

    @Throws(IOException::class, InterruptedException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val threadNumber = 2
        val threadPool = ThreadPoolExecutor(threadNumber, threadNumber,
                0L, TimeUnit.MILLISECONDS,
                LinkedBlockingQueue())

        val connectionFactory = ConnectionFactory()
        connectionFactory.host = "localhost"
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"

        val connection = connectionFactory.newConnection()
        val channel = connection.createChannel()

        logger.info(" [*] Waiting for messages. To exit press CTRL+C")

        registerConsumer(channel, 500, threadPool)

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Invoking shutdown hook...")
                logger.info("Shutting down thread pool...")
                threadPool.shutdown()
                try {
                    while (!threadPool.awaitTermination(10, TimeUnit.SECONDS));
                } catch (e: InterruptedException) {
                    logger.info("Interrupted while waiting for termination")
                }

                logger.info("Thread pool shut down.")
                logger.info("Done with shutdown hook.")
            }
        })
    }

    @Throws(IOException::class)
    private fun registerConsumer(channel: Channel, timeout: Int, threadPool: ExecutorService) {
        channel.exchangeDeclare(QUEUE_NAME, "fanout")
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "")

        val consumer = object : DefaultConsumer(channel) {
            @Throws(IOException::class)
            override fun handleDelivery(consumerTag: String?,
                                        envelope: Envelope?,
                                        properties: AMQP.BasicProperties?,
                                        body: ByteArray?) {
                try {
                    logger.info(String.format("Received (channel %d) %s", channel.channelNumber, String(body!!)))

                    threadPool.submit {
                        try {
                            Thread.sleep(timeout.toLong())
                            logger.info(String.format("Processed %s", String(body)))
                        } catch (e: InterruptedException) {
                            logger.warn(String.format("Interrupted %s", String(body)))
                        }
                    }
                } catch (e: Exception) {
                    logger.error("", e)
                }

            }
        }

        channel.basicConsume(QUEUE_NAME, true /* auto-ack */, consumer)
    }
}
