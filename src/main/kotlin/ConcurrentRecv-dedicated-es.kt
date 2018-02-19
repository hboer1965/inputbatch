
import com.rabbitmq.client.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

object ConcurrentRecv_dedicated_es {
    private val logger = LoggerFactory.getLogger(ConcurrentRecv::class.java)

    private val QUEUE_NAME = "hello"

    @Throws(IOException::class, InterruptedException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val connectionFactory = ConnectionFactory()
        connectionFactory.host = "localhost"
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"

        val executorService = Executors.newFixedThreadPool(9)
//        val connection = connectionFactory.newConnection(executorService)
        val connection = connectionFactory.newConnection()
        val channel = connection.createChannel()

        logger.info(" [*] Waiting for messages. To exit press CTRL+C")

        registerConsumer(channel, 500
                , executorService
        )
    }

    @Throws(IOException::class)
    private fun registerConsumer(channel: Channel, timeout: Int
                                 , executorService: ExecutorService
    ) {
        channel.exchangeDeclare(QUEUE_NAME, "fanout")
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "")

        val consumer = object : DefaultConsumer(channel) {
            val rnd = Random(5)
            @Throws(IOException::class)
            override fun handleDelivery(consumerTag: String?,
                                        envelope: Envelope?,
                                        properties: AMQP.BasicProperties?,
                                        body: ByteArray?) {
                if (rnd.nextInt(10) == 3) {
                        logger.info("Sleeping ${timeout}")
                        Thread.sleep(timeout.toLong())
                        }
                logger.info("Received (channel ${channel.channelNumber}) ${String(body!!)}")

                executorService.submit {
                    try {
                        Thread.sleep(100)
                        logger.info("Processed ${String(body)}")
                    } catch (e: InterruptedException) {
                    }
                }
            }
        }

        channel.basicConsume(QUEUE_NAME, true /* auto-ack */, consumer)
    }
}