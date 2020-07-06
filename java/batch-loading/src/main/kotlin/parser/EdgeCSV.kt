package parser

import model.*
import java.io.File
import java.nio.file.Path


class EdgeCSV(val filePath: Path, private val delimiter: Char) {
    private val file = File(filePath.toUri())

    fun getEdgesData(): Sequence<EdgeData> {
        val reader = file.bufferedReader()
        val header = parseHeader(reader.readLine(), delimiter)
        val labelIndex = header.columns.indexOfFirst { it.tag == Tag.LABEL }
        val startIdIndex = header.columns.indexOfFirst { it.tag == Tag.START_ID }
        val endIdIndex = header.columns.indexOfFirst { it.tag == Tag.END_ID }

        if (labelIndex == -1)
            throw IllegalStateException("File ${filePath.fileName} does not contain edge label column") //TODO allow empty labels
        if (startIdIndex == -1)
            throw IllegalStateException("File ${filePath.fileName} does not contain edge START_ID column")
        if (endIdIndex == -1)
            throw IllegalStateException("File ${filePath.fileName} does not contain edge END_ID column")

        return reader.lineSequence().map { line ->
            line.split(delimiter).let { data ->
                assert(data.size == header.columns.size) { "Line $line does not conform to header format" }
                val properties = header.columns.zip(data)
                EdgeData(
                    properties[startIdIndex].second.toLong(),
                    properties[endIdIndex].second.toLong(),
                    properties[labelIndex].second,
                    (properties - properties[startIdIndex] - properties[endIdIndex] - properties[labelIndex]).toMap()
                )
            }
        }
    }

    fun getSchema(): List<PropertyData> = parseHeader(
        file.bufferedReader().readLine(),
        delimiter
    ).columns.filterNot { it.tag == Tag.LABEL || it.tag == Tag.START_ID || it.tag == Tag.END_ID }

    fun getLabel(): String? = getEdgesData().first().label

    private fun parseHeader(text: String, delimiter: Char) = text.split(delimiter).map { column ->
        column.split(':').let { params ->
            if (params.size == 3) {
                val tag = params[2].substringBefore('(').let {
                    if (it.toUpperCase() == "TYPE")
                        Tag.LABEL
                    else
                        Tag.from(it)
                }

                PropertyData(params[0], FieldType.from(params[1]), tag)
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
                        it.substringBefore('(').let {
                            if (it.toUpperCase() == "TYPE")
                                Tag.LABEL
                            else
                                Tag.from(it)
                        }
                    } catch (e: IllegalArgumentException) {
                        null
                    }
                }

                PropertyData(params[0], type, tag)
            }
        }
    }.let(::EdgeHeader)

    data class EdgeHeader(val columns: List<PropertyData>)
}