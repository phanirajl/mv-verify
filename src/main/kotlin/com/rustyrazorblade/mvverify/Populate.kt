package com.rustyrazorblade.mvverify

import com.datastax.driver.core.ResultSetFuture
import java.util.Random


fun main(args: Array<String>) {

    var database = Database()

    var prepared = database.session.prepare("INSERT INTO base (k, v) VALUES (?, ?)")
    var random = Random()
    var results = mutableListOf<ResultSetFuture>()

    val iterations = 1000000
    val max = 10000

    val start = System.nanoTime()

    for(i in 1..iterations){
        val rand = random.nextInt(max)
        val rand2 = random.nextInt(max)

        val bound = prepared.bind()

        bound.setInt(0, rand)
        bound.setInt(1, rand2)

        var result  = database.session.executeAsync(bound)
        results.add(result)

        if(i % 500 == 0) {
            results.map { v -> v.uninterruptibly }
            println("Done with $i")
        }
    }

    val totalTime = System.nanoTime() - start

    println("Done. $totalTime")


}

