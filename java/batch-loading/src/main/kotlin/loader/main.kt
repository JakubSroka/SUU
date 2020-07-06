package loader

import connection.Connection
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import model.EdgeData
import model.PropertyData
import model.VertexData
import org.janusgraph.core.*
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import parser.EdgeCSV
import parser.VertexCSV
import java.io.File
import java.nio.file.Paths
import kotlin.concurrent.thread
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

@ExperimentalTime
fun main() {
    val nodeFiles = listOf(
        "output/comment_0_0.csv",
        "output/post_0_0.csv",
        "output/forum_0_0.csv",
        "output/tag_0_0.csv",
        "output/tagclass_0_0.csv",
        "output/place_0_0.csv",
        "output/person_0_0.csv",
        "output/organisation_0_0.csv"
    )
    val edgeFiles = listOf(
        "forum_hasModerator_person_0_0.csv",
        "person_isLocatedIn_place_0_0.csv",
        "post_hasCreator_person_0_0.csv",
        "comment_hasCreator_person_0_0.csv",
        "forum_hasTag_tag_0_0.csv",
        "person_knows_person_0_0.csv",
        "comment_hasTag_tag_0_0.csv",
        "person_likes_comment_0_0.csv",
        "comment_isLocatedIn_place_0_0.csv",
        "post_hasTag_tag_0_0.csv",
        "post_isLocatedIn_place_0_0.csv",
        "person_likes_post_0_0.csv",
        "comment_replyOf_comment_0_0.csv",
        "person_studyAt_organisation_0_0.csv",
        "tag_hasType_tagclass_0_0.csv",
        "comment_replyOf_post_0_0.csv",
        "person_workAt_organisation_0_0.csv",
        "organisation_isLocatedIn_place_0_0.csv",
        "tagclass_isSubclassOf_tagclass_0_0.csv",
        "forum_containerOf_post_0_0.csv",
        "place_isPartOf_place_0_0.csv",
        "forum_hasMember_person_0_0.csv",
        "person_hasInterest_tag_0_0.csv"
    ).map { "output/$it" }

    val delimiter = '|'
    val configFile = "configuration.properties"
    println(File(configFile).absolutePath)

    JanusGraphFactory.drop(Connection(configFile).graph)
    val conn = Connection(configFile)

    val nodes = nodeFiles.map { VertexCSV(Paths.get(it), delimiter) }
    val edges = edgeFiles.map { EdgeCSV(Paths.get(it), delimiter) }

    val propertySchema = (nodes.flatMap { it.getSchema() } + edges.flatMap { it.getSchema() }).distinct()
    conn.createSchema(propertySchema)
    nodes.mapNotNull { it.getLabel() }.distinct().forEach { conn.createVertexLabel(it) }
    edges.mapNotNull { it.getLabel() }.distinct().forEach { conn.createEdgeLabel(it) }

    val ids: List<Map<Long, Long>>
    measureTimedValue {
        ids = nodes.map { csv ->
            println("Processing vertex file ${csv.filePath.fileName}")
            conn.insertVertices(csv.getVerticesData()).also { println("Finished processing ${csv.filePath.fileName}") }
        }
    }.also { println("Inserting vertices took ${it.duration.inSeconds}s") }

    val idsSize = ids.map { it.size }.sum()
    println("Ids size: $idsSize")

    val idMapping = ids.fold(mutableMapOf<Long, Long>()) { acc, idMap -> acc.putAll(idMap); acc }.toMap()

    measureTimedValue {
        edges.map { csv ->
            thread {
                println("Processing edge file ${csv.filePath.fileName}")
                val connection = Connection(configFile)
                connection.insertEdges(csv.getEdgesData(), idMapping)
                    .also { println("Finished processing ${csv.filePath.fileName}") }
                connection.close()
            }
        }.forEach { it.join() }
    }.also { println("Inserting edges took ${it.duration.inSeconds} s") }

    println(conn.graph.openManagement().printSchema())
    conn.close()
}

fun Connection.insertVertices(vertices: Sequence<VertexData>): Map<Long, Long> {
    var transaction = graph.newTransaction()
    val idMapping = mutableMapOf<Long, Long>()

    for ((index, v) in vertices.withIndex()) {
        val vertex = transaction.addVertex(v.label)
        v.data.forEach { prop ->
            vertex.property(prop.key.name, prop.value)
        }
        v.ID?.let { oldID ->
            idMapping.put(oldID, vertex.id() as Long)
        }

        if ((index + 1) % 10000 == 0) {
            transaction.tx().commit()
            transaction.tx().close()
            transaction.close()
            transaction = graph.newTransaction()
            println("Created ${index + 1} vertices")
        }
    }
    transaction.tx().commit()
    transaction.tx().close()
    transaction.close()
    return idMapping
}

fun Connection.insertEdges(edges: Sequence<EdgeData>, idMapping: Map<Long, Long>) {
    var transaction = graph.newTransaction()

    for ((index, e) in edges.withIndex()) {
        val from = transaction.getVertex(idMapping[e.from] ?: error("Vertex with id ${e.from} not found in id map"))
        val to = transaction.getVertex(idMapping[e.to] ?: error("Vertex with id ${e.to} not found in id map"))
        if (from == null || to == null) {
            println("Cannot find vertices, skipping edge")
            continue
        }
        val edge = from.addEdge(e.label, to)
        e.data.forEach { prop ->
            edge.property(prop.key.name, prop.value)
        }

        if ((index + 1) % 10000 == 0) {
            transaction.commit()
            transaction.close()
            transaction = graph.newTransaction()
            println("Created ${index + 1} edges")
        }
    }
    transaction.commit()
    transaction.close()
}

fun Connection.createSchema(properties: Collection<PropertyData>) {
    val mgmt = graph.openManagement()
    properties.forEach {
        val propertyKey = mgmt.getPropertyKey(it.name)
        if (propertyKey != null) {
            println("Property already exists: ${it.name} - type ${propertyKey.dataType().simpleName}")
            if (propertyKey.dataType() != it.type.clazz)
                throw RuntimeException(it.name + ":" + it.type.clazz.simpleName + " mismatches existing datatype " + propertyKey.dataType().simpleName)

            if (propertyKey.cardinality() != Cardinality.SINGLE)
                throw RuntimeException(it.name + " isn't SINGLE cardinality")
        } else {
            println("Creating property ${it.name} - type ${it.type.clazz.simpleName}")
            mgmt.makePropertyKey(it.name).cardinality(Cardinality.SINGLE).dataType(it.type.clazz).make()
        }
    }
    mgmt.commit()
}

fun Connection.createVertexLabel(label: String) {
    val mgmt = graph.openManagement()
    val vertexLabel = mgmt.getVertexLabel(label)
    if (vertexLabel != null) {
        println("Label already exists: $label")
    } else {
        println("Creating vertex label $label")
        mgmt.makeVertexLabel(label).make()
    }
    mgmt.commit()
}

fun Connection.createEdgeLabel(label: String) {
    val mgmt = graph.openManagement()
    val edgeLabel = mgmt.getEdgeLabel(label)
    if (edgeLabel != null) {
        println("Label already exists: $label")
    } else {
        println("Creating edge label $label")
        mgmt.makeEdgeLabel(label).make()
    }
    mgmt.commit()
}