import com.rabbitmq.client.*
import org.slf4j.LoggerFactory
import java.io.IOException

object ConcurrentRecv {
    private val logger = LoggerFactory.getLogger(ConcurrentRecv::class.java)

    private val QUEUE_NAME = "hello"

    @Throws(IOException::class, InterruptedException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val connectionFactory = ConnectionFactory()
        connectionFactory.host = "localhost"
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"

        val connection = connectionFactory.newConnection()
        val channel = connection.createChannel()

        logger.info(" [*] Waiting for messages. To exit press CTRL+C")

        registerConsumer(channel, 500)
    }

    @Throws(IOException::class)
    private fun registerConsumer(channel: Channel, timeout: Int) {
        channel.exchangeDeclare(QUEUE_NAME, "fanout")
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "")

        val consumer = object : DefaultConsumer(channel) {
            @Throws(IOException::class)
            override fun handleDelivery(consumerTag: String?,
                                        envelope: Envelope?,
                                        properties: AMQP.BasicProperties?,
                                        body: ByteArray?) {
                logger.info(String.format("Received (channel %d) %s",
                        channel.channelNumber,
                        String(body!!)))

                try {
                    Thread.sleep(timeout.toLong())
                } catch (e: InterruptedException) {
                }

            }
        }

        channel.basicConsume(QUEUE_NAME, true /* auto-ack */, consumer)
    }
}