from cassandra.cluster import Cluster

cluster =Cluster(['localhost'])


connection=cluster.connect()


query="""
    SELECT * FROM users.users
"""

out=connection.execute(query=query)

for i in out:
    print(i)
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")