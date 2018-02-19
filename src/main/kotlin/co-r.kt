
import kotlin.coroutines.experimental.buildSequence

// inferred type is Sequence<Int>
val fibonacci = buildSequence {
    yield(1) // first Fibonacci number
    var cur = 1
    var next = 1
    while (true) {
        yield(next) // next Fibonacci number
        val tmp = cur + next
        cur = next
        next = tmp
    }
}

fun main(args: Array<String>) {
    println(fibonacci.take(10).joinToString())
}