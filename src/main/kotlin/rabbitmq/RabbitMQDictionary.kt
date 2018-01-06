package nl.tkp.ipde.rabbitmq

class RabbitMQDictionary {

    companion object {
        val logicalTargetToAMQQueueName = mapOf("rekenaar" to "ipde.rekenaar.in-queue")
        val logicalTargetToAMQExchangeName = mapOf("rekenaar" to "rekenaar-exchange")
    }
}