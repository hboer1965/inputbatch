package nl.tkp.ipde.rabbitmq

import com.rabbitmq.client.Channel
import com.rabbitmq.client.Consumer

class RabbitMQConsumer(private val channel: Channel, logicalTarget: String) {

    private val exchangeName = RabbitMQDictionary.logicalTargetToAMQExchangeName[logicalTarget]
    private val amqQueueName = RabbitMQDictionary.logicalTargetToAMQQueueName[logicalTarget]

    fun startConsumeWithConsumer(consumer: Consumer) {
        channel.queueDeclare(amqQueueName, false, false, true, null)
        channel.exchangeDeclare(exchangeName, "fanout", false)

        channel.queueBind(amqQueueName, exchangeName, "")
        channel.basicConsume(amqQueueName, false, consumer)
    }
}