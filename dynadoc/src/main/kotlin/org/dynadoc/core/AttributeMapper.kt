package org.dynadoc.core

import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter
import software.amazon.awssdk.protocols.jsoncore.JsonNode
import software.amazon.awssdk.protocols.jsoncore.JsonNodeParser
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

internal object AttributeMapper {
    const val PARTITION_KEY = "partition_key"
    const val SORT_KEY = "sort_key"
    const val VERSION = "version"

    private val specialKeys: Set<String> = setOf(PARTITION_KEY, SORT_KEY, VERSION)
    private val jsonAttributeConverter: JsonItemAttributeConverter = JsonItemAttributeConverter.create()
    private val jsonNodeParser: ThreadLocal<JsonNodeParser> = ThreadLocal.withInitial {
        JsonNodeParser.builder()
            .jsonFactory(JsonNodeParser.DEFAULT_JSON_FACTORY)
            .build()
    }

    fun toDocument(attributes: Map<String, AttributeValue>): Document {
        val bodyMap: Map<String, AttributeValue> = attributes.filterKeys { it !in specialKeys }
        val body: String? =
            if (bodyMap.isEmpty()) {
                null
            } else {
                val json: JsonNode = jsonAttributeConverter.transformTo(AttributeValue.fromM(bodyMap))
                json.toString()
            }

        return Document(
            id = parseKey(attributes),
            body = body,
            version = attributes[VERSION]!!.n().toLong()
        )
    }

    fun fromDocument(document: Document): Map<String, AttributeValue> = buildMap {
        if (document.body != null) {
            val jsonNode: JsonNode = jsonNodeParser.get().parse(document.body)
            val attributesRoot: AttributeValue = jsonAttributeConverter.transformFrom(jsonNode)

            putAll(attributesRoot.m())
        }

        putAll(getKeyAttributes(document.id))
        put(VERSION, AttributeValue.fromN((document.version + 1).toString()))
    }

    fun getKeyAttributes(id: DocumentKey) = mapOf(
        PARTITION_KEY to AttributeValue.fromS(id.partitionKey),
        SORT_KEY to AttributeValue.fromS(id.sortKey)
    )

    fun parseKey(attributes: Map<String, AttributeValue>): DocumentKey = DocumentKey(
        partitionKey = attributes[PARTITION_KEY]!!.s(),
        sortKey = attributes[SORT_KEY]!!.s()
    )
}