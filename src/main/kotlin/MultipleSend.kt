import com.rabbitmq.client.ConnectionFactory
import org.slf4j.LoggerFactory
import java.io.IOException

object MultipleSend {
    private val logger = LoggerFactory.getLogger(MultipleSend::class.java)

    private val QUEUE_NAME = "hello"

    @Throws(IOException::class)
    @JvmStatic
    fun main(args: Array<String>) {
        val connectionFactory = ConnectionFactory()
        connectionFactory.host = "localhost"
        connectionFactory.username = "admin"
        connectionFactory.password = "rabbitmq_pass"

        val connection = connectionFactory.newConnection()
        val channel = connection.createChannel()

        channel.exchangeDeclare(QUEUE_NAME, "fanout")
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "")

        for (i in 1..5_000) {
            val message = "Hello world" + i
            channel.basicPublish("", QUEUE_NAME, null, message.toByteArray())
//            logger.info(" [x] Sent '$message'")
        }

        channel.close()
        connection.close()
    }
}
