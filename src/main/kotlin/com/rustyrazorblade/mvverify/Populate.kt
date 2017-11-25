package com.rustyrazorblade.mvverify

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.QueryOptions
import com.datastax.driver.core.ResultSetFuture
import java.util.Random

fun main(args: Array<String>) {

    var options = QueryOptions()
    options.consistencyLevel = ConsistencyLevel.ONE

    var cluster = Cluster.builder()
                         .addContactPoint("127.0.0.1")
                         .withQueryOptions(options)
                         .build()
    var session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS mvtest " +
                    "WITH replication = " +
                    "{'class': 'SimpleStrategy', " +
                    "'replication_factor': 3}")
    session.execute("USE mvtest")

    session.execute("CREATE table IF NOT EXISTS base ( " +
                    "k int primary key, " +
                    "v int " +
                    ")")

    session.execute("CREATE MATERIALIZED VIEW if not exists mv AS " +
                    "SELECT v, k FROM base " +
                    "WHERE v is not null " +
                    "primary key (v, k)")

    var prepared = session.prepare("INSERT INTO base (k, v) VALUES (?, ?)")
    var random = Random()
    var results = mutableListOf<ResultSetFuture>()

    val iterations = 1000000
    val max = 10000

    for(i in 1..iterations){
        val rand = random.nextInt(max)
        val rand2 = random.nextInt(max)

        val bound = prepared.bind()

        bound.setInt(0, rand)
        bound.setInt(1, rand2)

        var result  = session.executeAsync(bound)
        results.add(result)

        if(i % 500 == 0) {
            results.map { v -> v.uninterruptibly }
            println("Done with $i")
        }
    }

    println("Done.")


}

