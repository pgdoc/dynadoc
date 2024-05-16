package org.dynadoc

data class Document(
    val id: DocumentKey,
    val body: String?,
    val version: Long,
    val extraAttributes: Map<String, String> = emptyMap()
)

data class DocumentKey(
    val partitionKey: String,
    val sortKey: String
) {
    override fun toString(): String {
        return "(\"$partitionKey\", \"$sortKey\")"
    }
}
