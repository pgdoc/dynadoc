package org.dynadoc.serialization

import org.dynadoc.core.Document

interface JsonSerializer {
    /**
     * Serializes the specified object to a JSON string.
     */
    fun serialize(entity: Any): String

    /**
     * Deserializes a JSON string to the specified type.
     */
    fun <T : Any> deserialize(json: String, clazz: Class<T>): T
}


fun JsonSerializer.toDocument(jsonEntity: JsonEntity<Any?>): Document =
    Document(
        id = jsonEntity.id,
        body = jsonEntity.entity?.let { serialize(it) },
        version = jsonEntity.version
    )

fun <T : Any> JsonSerializer.fromDocument(document: Document, clazz: Class<T>): JsonEntity<T?> =
    JsonEntity(
        id = document.id,
        entity = document.body?.let { deserialize(it, clazz) },
        version = document.version
    )

inline fun <reified T : Any> JsonSerializer.fromDocument(document: Document): JsonEntity<T?> =
    fromDocument(document, T::class.java)
