package com.rustyrazorblade.mvverify

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.ConsistencyLevel
import com.datastax.driver.core.QueryOptions
import com.datastax.driver.core.Session

class Database {

    var cluster: Cluster
    var session: Session

    init {

        var options = QueryOptions()
        options.consistencyLevel = ConsistencyLevel.ONE

        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withQueryOptions(options)
                .build()
        session = cluster.connect()

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
    }
}