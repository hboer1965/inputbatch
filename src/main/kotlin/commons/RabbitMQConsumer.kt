package commons

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.Channel
import com.rabbitmq.client.DefaultConsumer
import com.rabbitmq.client.Envelope

data class QueueMessage(val body: ByteArray, val correlationId: String?, val replyTo:String?, val properties: AMQP.BasicProperties?)

class RabbitMQConsumer(private val rmqChannel: Channel, private val whatToDo: (QueueMessage) -> Unit) : DefaultConsumer(rmqChannel) {

    override fun handleDelivery(consumerTag: String?, envelope: Envelope, properties: AMQP.BasicProperties?, body: ByteArray) {
        val qm = QueueMessage(
                body = body,
                correlationId = properties?.correlationId,
                replyTo = properties?.replyTo,
                properties = properties)
        whatToDo(qm)
    }
}