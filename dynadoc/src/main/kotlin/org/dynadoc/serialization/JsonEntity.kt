package org.dynadoc.serialization

import org.dynadoc.DocumentKey

data class JsonEntity<out T>(
    val id: DocumentKey,
    val entity: T,
    val version: Long
)

fun <T> JsonEntity<T>.modify(builder: T.() -> T) =
    JsonEntity(id, builder(entity), version)