package com.rustyrazorblade.mvverify

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.DataType
import com.datastax.driver.core.TypeCodec
import com.datastax.driver.core.TypeTokens

fun main(args: Array<String>) {
    println("Running verify")
    var cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
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
            println("Error, MV row not found")
        }
        checked++
        if(checked % 100 == 0) {
            println("Checked $checked records.")
        }
    }
    println("Finished checking MV against base table.")

    println("Checking base table against MV data.")

}


fun getPreparedQuery(name: String, keys: List<String>) : String {
    val where = keys.map { value -> "$value = :$value" }.joinToString(" AND ")
    val preparedQuery = "SELECT * from $name WHERE $where"
    return preparedQuery

}