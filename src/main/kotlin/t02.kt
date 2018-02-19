
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.Channel
import java.time.LocalDateTime
import java.util.*
import kotlin.system.measureTimeMillis

data class Vraag(val id: Int, val optid: String)

val randomGen = Random(27654)

val vragen = listOf(Vraag(1, "A"),
        Vraag( 2, "B"),
        Vraag( 3, "C"),
        Vraag( 4, "D"),
        Vraag( 5, "E"),
        Vraag( 6, "F"),
        Vraag( 7, "A"),
        Vraag( 8, "B"),
        Vraag( 9, "C"),
        Vraag(10, "D"),
        Vraag(11, "E"),
        Vraag(12, "F"),
        Vraag(13, "A"),
        Vraag(14, "B"),
        Vraag(15, "C"),
        Vraag(16, "D"),
        Vraag(17, "E"),
        Vraag(18, "F"),
        Vraag(19, "A"),
        Vraag(20, "A")
)

data class Optrggid(val optid: String, val rggid: Int)

data class Optrgg(val optid: String, val rgg: String)

val optrggmap = mapOf("A" to 10, "B" to 20, "C" to 30, "D" to 40, "E" to 50, "F" to 60)

fun log(s: String) = println("${LocalDateTime.now()} - " +
        s + " - thread=${Thread.currentThread().name}.")

suspend fun produceVragenvanJob(optidChannel: Channel<String>, vraagChannel: Channel<Vraag>) {
    log("produceVragenvanJob START")
    vragen.forEach {vraag ->
        delay(1+randomGen.nextInt(10))
        log("fetch vraag it=${vraag}")
        optidChannel.send(vraag.optid)
        vraagChannel.send(vraag)
    }
    optidChannel.close()
    vraagChannel.close()
    log("produceVragenvanJob END")
}


suspend fun produceOneRggid(optid: String, channel: Channel<Optrggid>)  {
    val delay = 10+randomGen.nextInt(100)
    delay(delay)
    channel.send(Optrggid(optid, optrggmap[optid]!!))
    log("produceOneRggid; optid=${optid} - delayed $delay milliseconds")
}


suspend fun produceOneRegeling(optid: String, rggid: Int, channel: Channel<Optrgg>) {
    val delay = 10+randomGen.nextInt(100)
    delay(delay)
    channel.send(Optrgg(optid, "Regeling $rggid"))
    log("produceOneRegeling; optid=${optid}; rggid=${rggid} - delayed for $delay milliseconds")
}

suspend fun produceAlleRggids(receiveChannel: Channel<String>, sendChannel: Channel<Optrggid>) {
    val bekendeOptIds = mutableSetOf<String>()
    val rggidJobs = mutableListOf<Job>()

    for (optid in receiveChannel)  {
        log("produceAlleRggids read receiveChannel; optid=${optid}")
        if (!bekendeOptIds.contains(optid)) {
            bekendeOptIds.add(optid)
            log("calling produceOneRggid with it.optid=${optid}")
            rggidJobs += launch { produceOneRggid(optid, sendChannel) }
        }
    }
    rggidJobs.forEach { it.join() }
    sendChannel.close()
}

suspend fun produceAlleRggs(receiveChannel: Channel<Optrggid>, sendChannel: Channel<Optrgg>) {
    val rggJobs = mutableListOf<Job>()
    val bekendeRggIds = mutableSetOf<Optrggid>()

    for (rggid in receiveChannel) {
        if (!bekendeRggIds.contains(rggid)) {
            bekendeRggIds.add(rggid)
            rggJobs += launch { produceOneRegeling(rggid.optid, rggid.rggid, sendChannel) }
        }
    }
    rggJobs.forEach { it.join() }
    sendChannel.close()
}

suspend fun postAlleVragen(vragenChannelforPosting: Channel<Vraag>, rggChannel: Channel<Optrgg>) {
    val retryVragen = mutableListOf<Vraag>()
    var vragenCount = 0
    var vragenPosted = 0

    val firstRgg = rggChannel.receive()  // wait for first arrival
    val gemapteOptIds = mutableMapOf(Pair(firstRgg.optid, firstRgg))

    // in case there have been arrived more entries
    while (rggChannel.poll()?.let {gemapteOptIds.put(it.optid, it); true} ?: false) {
        log("polled and got one or more!")
    }
    // Just for now received regeling
    for (vraag in vragenChannelforPosting) {
        vragenCount++
        if (gemapteOptIds.contains(vraag.optid)) {
            log("POSTING id=${vraag.id}; optid=${vraag.optid}; rgg=${gemapteOptIds[vraag.optid]!!.rgg}")
            vragenPosted++
        } else {
            retryVragen += vraag
        }
    }
    //The remaining regelingen
    for (rgg in rggChannel) {
        gemapteOptIds.put(rgg.optid, rgg)
        retryVragen.filter { it.optid == rgg.optid }
                .forEach {
                    log("POSTING id=${it.id}; optid=${it.optid}; rgg=${gemapteOptIds[it.optid]!!.rgg}")
                    vragenPosted++
                }
    }
    if (vragenCount != vragenPosted) {
        throw Exception("Vragen posted != vragen sent ($vragenPosted!=$vragenCount)and rggChannel is closed!")
    } else {
        log("DONE vragen posted == vragen sent ($vragenPosted==$vragenCount)and rggChannel is closed.")
    }
}


fun main(args: Array<String>) = runBlocking<Unit> {

    log("MAIN start")
    val vragenChannelforRegelingIds = Channel<String>(Channel.UNLIMITED)
    val vragenChannelforPosting = Channel<Vraag>(Channel.UNLIMITED)
    val rggidChannel = Channel<Optrggid>(Channel.UNLIMITED)
    val rggChannel = Channel<Optrgg>(Channel.UNLIMITED)

    log("MAIN just before try")
    try { withTimeout(500L) {
            val duration = measureTimeMillis {
                launch(coroutineContext) { produceVragenvanJob(vragenChannelforRegelingIds, vragenChannelforPosting) }

                launch { produceAlleRggids(vragenChannelforRegelingIds, rggidChannel) }

                launch { produceAlleRggs(rggidChannel, rggChannel) }

                val postJob = launch {
                    postAlleVragen(vragenChannelforPosting, rggChannel)
                }

                postJob.join()
            }
            log("That took $duration milliseconds.")
        }
    } catch (tce: TimeoutCancellationException) {
        log("UNFORTUNATELY - not all done within time: ${tce.message}.")
    }
}

