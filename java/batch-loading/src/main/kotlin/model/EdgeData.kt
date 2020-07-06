package model

data class EdgeData(val from: Long, val to: Long, val label: String?, val data: Map<PropertyData, String>)
