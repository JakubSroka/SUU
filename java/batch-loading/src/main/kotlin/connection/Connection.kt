package connection

import org.janusgraph.core.JanusGraph
import org.janusgraph.core.JanusGraphFactory

data class Connection(val configFile: String) {
    val graph: JanusGraph

    init {
        println("Opening graph from information in $configFile")
        graph = JanusGraphFactory.open(configFile)
    }

    fun close() {
        try {
            graph.close()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }
}