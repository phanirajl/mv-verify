package com.rustyrazorblade.mvverify

fun main(args: Array<String>) {
    println("Running verify")

    var errors = 0
    var database = Database()

    var keyspace =  database.cluster.metadata.getKeyspace("mvtest")
    val mv = keyspace.getMaterializedView("mv")
    val mvPk = mv.primaryKey

    var names = mutableListOf<String>()

    for(key in mvPk) {
        names.add(key.name)
    }

    val preparedQuery = getPreparedQuery("mv", names)
    println("Preparing: $preparedQuery")

    val scanQuery = "SELECT * from base"

    val prepared = database.session.prepare(preparedQuery)

    var checked = 0
    for (row in database.session.execute(scanQuery)) {
        var bound = prepared.bind()
        for(field in mvPk) {
            var mvValue = row.getInt(field.name)
            bound.setInt(field.name, mvValue)
        }

        var mvMatch = database.session.execute(bound)

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

    var baseTable = keyspace.getTable("base")
    var basePk = baseTable.primaryKey

    var baseNames = mutableListOf<String>()

    for(key in basePk) {
        baseNames.add(key.name)
    }

    val basePreparedQuery = "SELECT * from base WHERE k = ?"
            //getPreparedQuery("base", names)
    println("Preparing $basePreparedQuery")
    var preparedBaseCheck = database.session.prepare(basePreparedQuery)

    for (row in database.session.execute("SELECT k, v from mv")) {
        var bound = preparedBaseCheck.bind()

        val k = row.getInt("k")
        val v = row.getInt("v")

        bound.setInt(0, k)
//        bound.setInt(1, v)

        var mvMatch = database.session.execute(bound).one()


        if (mvMatch == null ) {
            println("Error, base row not found, k=$k, v=$v")
            errors++
        } else if(mvMatch.getInt("v") != v ){
            println("Error, wrong value found, k=$k, v=$v")
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