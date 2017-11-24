from cassandra.cluster import Cluster
import random
import sys

c = Cluster(["127.0.0.1"])

session = c.connect()

schema = [
"""
CREATE table IF NOT EXISTS base (
k int primary key,
v int
);
""",

"""
CREATE MATERIALIZED VIEW if not exists mv AS
SELECT v, k FROM base
WHERE v is not null
primary key (v, k)
""",
]

session.execute("""CREATE KEYSPACE IF NOT EXISTS mvtest
                    WITH replication =
                    {'class': 'SimpleStrategy', 'replication_factor': 3}""")
session.execute("USE mvtest")

for query in schema:
    session.execute(query)

prepared = session.prepare("INSERT INTO base (k, v) VALUES (?, ?)")

futures = []
total = 10000
for x in range(total):
    bound = prepared.bind((random.randint(0, 1000), random.randint(0, 100)))
    f = session.execute_async(bound, prepared)
    futures.append(f)
    if len(futures) == 100:
        [y.result() for y in futures]
        futures = []
        tmp = "{} of {}, {}%\r".format(x, total, float(x) / total)
        sys.stdout.write(tmp)
        sys.stdout.flush()

[x.result() for x in futures]
# verify the data

print ""
print "Done."
