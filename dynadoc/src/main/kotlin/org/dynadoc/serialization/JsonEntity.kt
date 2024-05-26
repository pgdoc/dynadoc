package org.dynadoc.serialization

import org.dynadoc.core.DocumentKey

/**
 * Represents a document composed of a unique ID, a deserialized JSON body and a version number.
 */
data class JsonEntity<out T>(
    /** The unique identifier of the document. **/
    val id: DocumentKey,

    /** The body of the document deserialized into an object, or null if the document does not exist. **/
    val entity: T,

    /** The current version of the document. **/
    val version: Long
)


fun <T> JsonEntity<T>.modify(builder: T.() -> T) = copy(entity = builder(entity))

fun <T> createEntity(partitionKey: String, sortKey: String, entity: T) =
    JsonEntity(DocumentKey(partitionKey, sortKey), entity, 0)
