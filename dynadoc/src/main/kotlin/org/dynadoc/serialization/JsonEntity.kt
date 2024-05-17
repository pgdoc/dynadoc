package org.dynadoc.serialization

import org.dynadoc.DocumentKey

data class JsonEntity<out T>(
    val id: DocumentKey,
    val entity: T,
    val version: Long
)
