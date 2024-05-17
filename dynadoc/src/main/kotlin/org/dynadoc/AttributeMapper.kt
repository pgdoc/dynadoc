package org.dynadoc

import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue

internal object AttributeMapper {
    const val PARTITION_KEY = "partition_key"
    const val SORT_KEY = "sort_key"
    const val VERSION = "version"
    private val specialKeys = setOf(PARTITION_KEY, SORT_KEY, VERSION)

    fun toDocument(attributes: Map<String, AttributeValue>): Document {
        val bodyMap = attributes.filterKeys { it !in specialKeys }
        val body =
            if (bodyMap.isEmpty()) {
                null
            } else {
                val rootMap = bodyMap.entries.associate { it.key to readAttribute(it.value) }
                val objectMapper: ObjectMapper = jacksonObjectMapper()

                objectMapper.writeValueAsString(rootMap)
            }

        return Document(
            id = parseKey(attributes),
            body = body,
            version = attributes[VERSION]!!.asN().toLong()
        )
    }

    fun fromDocument(document: Document): Map<String, AttributeValue> = buildMap {
        if (document.body != null) {
            val objectMapper: ObjectMapper = jacksonObjectMapper()
            val jsonMap: Map<String, Any> = objectMapper.readValue(document.body)
            val root = writeAttribute(jsonMap) as AttributeValue.M

            putAll(root.value)
        }

        putAll(getKeyAttributes(document.id))
        put(VERSION, AttributeValue.N((document.version + 1).toString()))
    }

    fun getKeyAttributes(id: DocumentKey) = mapOf(
        PARTITION_KEY to AttributeValue.S(id.partitionKey),
        SORT_KEY to AttributeValue.S(id.sortKey)
    )

    fun parseKey(attributes: Map<String, AttributeValue>): DocumentKey = DocumentKey(
        partitionKey = attributes[PARTITION_KEY]!!.asS(),
        sortKey = attributes[SORT_KEY]!!.asS()
    )

    private fun writeAttribute(value: Any?): AttributeValue {
        if (value == null) {
            return AttributeValue.Null(true)
        }

        return when (value) {
            is String -> AttributeValue.S(value)
            is Number -> AttributeValue.N(value.toString())
            is Boolean -> AttributeValue.Bool(value)
            is List<*> -> AttributeValue.L(value.map { writeAttribute(it) })
            is Map<*, *> -> AttributeValue.M(value.entries.associate { (k, v) -> k as String to writeAttribute(v) })
            else -> throw RuntimeException("Unknown type ${value::class.qualifiedName}")
        }
    }

    private fun readAttribute(value: AttributeValue): Any? {
        return when (value) {
            is AttributeValue.S -> value.value
            is AttributeValue.N -> value.value.toLong()
            is AttributeValue.Bool -> value.value
            is AttributeValue.L -> value.value.map { readAttribute(it) }
            is AttributeValue.M -> value.value.entries.associate { it.key to readAttribute(it.value) }
            is AttributeValue.Null -> null
            else -> throw RuntimeException("Unknown type ${value::class.qualifiedName}")
        }
    }
}