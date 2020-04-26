import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Example {
    public static void main(String[] args) {
        GraphTraversalSource g;
        try {
            g = traversal().withRemote("remote-graph.properties");
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        System.out.println("Vertex count: " + g.V().count().next());

        Vertex v = g.addV().property("foo", "bar").property("baz", "qux").next(); // Add a new vertex

        System.out.println("Vertex properties: " + g.V(v.id()).valueMap().next());
        System.out.println("Vertex count: " + g.V().count().next());

        try {
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
