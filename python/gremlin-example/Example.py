from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

g = traversal().withRemote(DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

print("Vertex count: ", g.V().count().next())

v = g.addV().property("foo", "bar").property("baz", "qux").next()

print("Vertex properties: ", g.V(v.id).valueMap().next())
print("Vertex count: ", g.V().count().next())
