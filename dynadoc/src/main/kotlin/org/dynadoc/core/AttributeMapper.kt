package org.dynadoc.core

import software.amazon.awssdk.enhanced.dynamodb.internal.converter.attribute.JsonItemAttributeConverter
import software.amazon.awssdk.protocols.jsoncore.JsonNode
import software.amazon.awssdk.protocols.jsoncore.JsonNodeParser
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

object AttributeMapper {
    const val PARTITION_KEY = "partition_key"
    const val SORT_KEY = "sort_key"
    const val VERSION = "version"
    const val DELETED = "is_deleted"
    val systemAttributes: Set<String> = setOf(PARTITION_KEY, SORT_KEY, VERSION, DELETED)

    private val jsonAttributeConverter: JsonItemAttributeConverter = JsonItemAttributeConverter.create()
    private val jsonNodeParser: ThreadLocal<JsonNodeParser> = ThreadLocal.withInitial {
        JsonNodeParser.builder()
            .jsonFactory(JsonNodeParser.DEFAULT_JSON_FACTORY)
            .build()
    }

    fun toDocument(attributes: Map<String, AttributeValue>): Document {
        val body: String? =
            if (attributes[DELETED]!!.bool() == true) {
                null
            } else {
                val bodyMap: Map<String, AttributeValue> = attributes.filterKeys { it !in systemAttributes }
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
            require(attributesRoot.type() == AttributeValue.Type.M) { "The document must be a JSON object." }

            val specialAttribute: String? = systemAttributes.firstOrNull() { key -> attributesRoot.m().containsKey(key) }
            require(specialAttribute == null) {
                "The document cannot use the special attribute \"$specialAttribute\"."
            }

            putAll(attributesRoot.m())
        }

        putAll(getKeyAttributes(document.id))
        put(VERSION, AttributeValue.fromN((document.version + 1).toString()))
        put(DELETED, AttributeValue.fromBool(document.body == null))
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