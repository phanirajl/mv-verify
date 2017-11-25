package com.rustyrazorblade.mvverify

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.QueryOptions

fun main(args: Array<String>) {
    println("Running verify")

    var errors = 0

    var options = QueryOptions()
    options.consistencyLevel = ConsistencyLevel.ONE

    var cluster = Cluster.builder().addContactPoint("127.0.0.1").withQueryOptions(options).build()
    var session = cluster.connect("mvtest")

    var keyspace =  cluster.metadata.getKeyspace("mvtest")
    val mv = keyspace.getMaterializedView("mv")
    val mvPk = mv.primaryKey

    var names = mutableListOf<String>()

    for(key in mvPk) {
        names.add(key.name)
    }

    val preparedQuery = getPreparedQuery("mv", names)
    println("Preparing: $preparedQuery")

    val scanQuery = "SELECT * from base"

    val prepared = session.prepare(preparedQuery)

    var checked = 0
    for (row in session.execute(scanQuery)) {
        var bound = prepared.bind()
        for(field in mvPk) {
            var mvValue = row.getInt(field.name)
            bound.setInt(field.name, mvValue)
        }

        var mvMatch = session.execute(bound)

        if (mvMatch.count() == 0) {
            println("Error, MV row not found $mvPk")
            errors++
        }
        checked++
        if(checked % 100 == 0) {
            println("Checked $checked records.")
        }
    }
    println("Finished checking MV against base table.")

    println("Checking base table against MV data.")

    val mvScanQuery = "SELECT * from mv"

    var baseTable = keyspace.getTable("base")
    var basePk = baseTable.primaryKey

    var baseNames = mutableListOf<String>()

    for(key in basePk) {
        baseNames.add(key.name)
    }

    val basePreparedQuery = getPreparedQuery("mv", names)
    println("Preparing $basePreparedQuery")
    var preparedBaseCheck = session.prepare(basePreparedQuery)

    for (row in session.execute(mvScanQuery)) {
        var bound = preparedBaseCheck.bind()

        val k = row.getInt("k")
        val v = row.getInt("v")

        bound.setInt(0, k)
        bound.setInt(1, v)

        var mvMatch = session.execute(bound)

        if (mvMatch.count() == 0) {
            println("Error, base row not found, k=$k, v=$v")
            errors++
        }
        checked++
        if(checked % 100 == 0) {
            println("Checked $checked records.")
        }


    }

    println("Done scanning MV and checking base.  Total errors: $errors")


}


fun getPreparedQuery(name: String, keys: List<String>) : String {
    val where = keys.map { value -> "$value = :$value" }.joinToString(" AND ")
    val preparedQuery = "SELECT * from $name WHERE $where"
    return preparedQuery

}