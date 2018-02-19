import com.rabbitmq.http.client.Client

fun main(args: Array<String>) {
    val conn = Client("http://127.0.0.1:15672/api/", "admin", "rabbitmq_pas")
    val ov = conn.getOverview()
    val q = conn.getQueue("/", "abc")
    val h = conn.getQueue("/", "hello")

    println(ov.clusterName)
    println(ov.queueTotals.messagesReady)
    print(h)

    if (h.messagesReady < 500_000) {
        println("enough capacity left for adding messages (${h.messagesReady}<500_000")
    }
}

