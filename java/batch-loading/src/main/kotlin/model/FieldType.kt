package model

import java.util.*

enum class FieldType(val clazz: Class<*>) {
    ID(java.lang.String::class.java),
    STRING(java.lang.String::class.java),
    INT(java.lang.Integer::class.java),
    LONG(java.lang.Long::class.java),
    FLOAT(java.lang.Float::class.java),
    DOUBLE(java.lang.Double::class.java),
    BOOLEAN(java.lang.Boolean::class.java),
    BYTE(java.lang.Byte::class.java),
    SHORT(java.lang.Short::class.java),
    CHAR(Char::class.java),
    DATETIME(Date::class.java),
    UUID(java.util.UUID::class.java),
    DATE(Date::class.java);

    companion object {
        fun from(text: String) = valueOf(text.toUpperCase())
    }
}