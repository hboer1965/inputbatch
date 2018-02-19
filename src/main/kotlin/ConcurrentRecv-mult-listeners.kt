
import com.rabbitmq.client.*
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.Executors
import kotlin.concurrent.thread

object ConcurrentRecv_mult_listeners {
    private val logger = LoggerFactory.getLogger(ConcurrentRecv_mult_listeners::class.java)

    private val QUEUE_NAME = "hello"

    @JvmStatic
    fun main(args: Array<String>) {
        val connectionFactory = ConnectionFactory()
        connectionFactory.host = "localhost"
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"

        val executorService = Executors.newFixedThreadPool(100)
//        val connection = connectionFactory.newConnection()
        val connection = connectionFactory.newConnection(executorService)

        logger.info(" [*] Waiting for messages. To exit press CTRL+C")

        repeat (100) {
            thread(isDaemon = true) {

                val channel = connection.createChannel()
                channel.basicQos(20)
                registerConsumer(channel, 500)
            }
        }
    }

    private fun registerConsumer(channel: Channel, timeout: Long) {
        channel.exchangeDeclare(QUEUE_NAME, "fanout")
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "")

        val consumer = object : DefaultConsumer(channel) {
            @Throws(IOException::class)
            override fun handleDelivery(consumerTag: String,
                                        envelope: Envelope,
                                        properties: AMQP.BasicProperties?,
                                        body: ByteArray) {
                logger.info("Received (channel ${channel.channelNumber}) ${String(body)}")
                Thread.sleep(timeout)
                channel.basicAck(envelope.deliveryTag, false)
            }
        }

        channel.basicConsume(QUEUE_NAME, false /* auto-ack */, consumer)
    }
}