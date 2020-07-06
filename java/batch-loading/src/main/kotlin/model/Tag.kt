package model


enum class Tag {
    ID, START_ID, END_ID, DATA, INDEX, UNIQUE, IGNORE, LABEL;

    companion object {
        fun from(text: String) = valueOf(text.toUpperCase())
    }
}

