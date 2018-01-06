package Resultswriter

import commons.BrokerConnection
import commons.Results
import commons.ResultsConnection
import kotlinx.coroutines.experimental.channels.Channel
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking

import org.slf4j.LoggerFactory

class Application {
    private val logger = LoggerFactory.getLogger(Application::class.java)!!
//    private val config = Config(null)

    private val resultsConnection = ResultsConnection("jdbc:oracle:thin:@localhost:1521/XE", "system", "oracle")
    private val consumeConn = BrokerConnection("localhost", "admin", "rabbitmq_pass")

    val consumeChannel = Channel<Results>(20)
    fun start(output: String) {
        println("Starting application ResultsWriter to ${output}...")

        runBlocking {
            consumeConn.ConsumingResults(consumeChannel).start()
            while (true) {
                delay(100) // so we can insert efficient with arrays
                val resultList = mutableListOf<Results>()
                while (!consumeChannel.isEmpty && resultList.size <= 20) {
                    resultList.add(consumeChannel.receive())
                }
                resultsConnection.insertIpdeResponses(resultList)
            }
        }
    }
}

fun main(args: Array<String>) {
    Application().start("ukd")
}