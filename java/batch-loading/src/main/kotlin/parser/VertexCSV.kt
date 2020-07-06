package parser

import model.FieldType
import model.PropertyData
import model.Tag
import model.VertexData
import java.io.File
import java.nio.file.Path


class VertexCSV(val filePath: Path, private val delimiter: Char) {
    private val file = File(filePath.toUri())

    fun getVerticesData(): Sequence<VertexData> {
        val reader = file.bufferedReader()
        val header = parseHeader(reader.readLine(), delimiter)
        val labelIndex = header.columns.indexOfFirst { it.tag == Tag.LABEL }
        val idIndex = header.columns.indexOfFirst { it.tag == Tag.ID }
        if (labelIndex == -1)
            throw IllegalStateException("File ${filePath.fileName} does not contain vertex label column") //TODO allow empty labels
        if (idIndex == -1)
            throw IllegalStateException("File ${filePath.fileName} does not contain vertex ID column") //TODO allow empty IDs


        return reader.lineSequence().map { line ->
            line.split(delimiter).let { data ->
                assert(data.size == header.columns.size) { "Line $line does not conform to header format" }
                val properties = header.columns.zip(data)
                VertexData(
                    properties[idIndex].second.toLong(),
                    properties[labelIndex].second,
                    (properties - properties[idIndex] - properties[labelIndex]).toMap()
                )
            }
        }
    }

    fun getSchema(): List<PropertyData> = parseHeader(
        file.bufferedReader().readLine(),
        delimiter
    ).columns.filterNot { it.tag == Tag.LABEL || it.tag == Tag.ID }

    fun getLabel(): String? = getVerticesData().first().label

    private fun parseHeader(text: String, delimiter: Char) = text.split(delimiter).map { column ->
        column.split(':').let { params ->
            if (params.size == 3) {
                PropertyData(
                    params[0],
                    FieldType.from(params[1]),
                    Tag.from(params[2].substringBefore('('))
                )
            } else {
                val tagOrLabel = params.getOrNull(1)
                val type = tagOrLabel?.let {
                    try {
                        FieldType.from(tagOrLabel)
                    } catch (e: IllegalArgumentException) {
                        FieldType.STRING
                    }
                } ?: FieldType.STRING

                val tag = tagOrLabel?.let {
                    try {
                        Tag.from(it.substringBefore('('))
                    } catch (e: IllegalArgumentException) {
                        null
                    }
                }

                PropertyData(params[0], type, tag)
            }
        }
    }.let(::VertexHeader)

    data class VertexHeader(val columns: List<PropertyData>)
}