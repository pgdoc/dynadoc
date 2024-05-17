package org.dynadoc.serialization

import org.dynadoc.Document

interface JsonSerializer {
    fun serialize(entity: Any): String

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
