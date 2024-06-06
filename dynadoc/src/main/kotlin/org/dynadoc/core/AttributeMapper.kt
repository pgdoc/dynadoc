package org.dynadoc.core

import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.io.UncheckedIOException
import java.lang.IllegalArgumentException
import java.time.Clock
import java.time.Duration
import java.time.Instant

const val PARTITION_KEY = "partition_key"
const val SORT_KEY = "sort_key"
const val VERSION = "version"
const val DELETED = "deleted"

val systemAttributes: Set<String> = setOf(PARTITION_KEY, SORT_KEY, VERSION, DELETED)

class AttributeMapper(
    private val expiration: Duration,
    private val clock: Clock
) {
    fun toDocument(attributes: Map<String, AttributeValue>): Document {
        val body: String? =
            if (attributes[DELETED] != null) {
                null
            } else {
                val bodyMap: Map<String, AttributeValue> = attributes.filterKeys { it !in systemAttributes }
                EnhancedDocument.fromAttributeValueMap(bodyMap).toJson()
            }

        return Document(
            id = toDocumentKey(attributes),
            body = body,
            version = attributes.getValue(VERSION).n().toLong()
        )
    }

    fun fromDocument(document: Document): Map<String, AttributeValue> = buildMap {
        if (document.body != null) {
            require(jsonObject.containsMatchIn(document.body)) {
                "The document must be a valid JSON object."
            }

            val attributes: Map<String, AttributeValue> = try {
                EnhancedDocument.fromJson(document.body).toMap()
            } catch (e: UncheckedIOException) {
                throw IllegalArgumentException("The document must be a valid JSON object.", e)
            }

            val specialAttribute: String? = systemAttributes.firstOrNull { key -> attributes.containsKey(key) }
            require(specialAttribute == null) {
                "The document cannot use the special attribute \"$specialAttribute\"."
            }

            putAll(attributes)
        }

        putAll(fromDocumentKey(document.id))
        put(VERSION, AttributeValue.fromN((document.version + 1).toString()))

        if (document.body == null) {
            val expiration: Instant = clock.instant() + expiration
            put(DELETED, AttributeValue.fromN(expiration.epochSecond.toString()))
        }
    }

    fun fromDocumentKey(id: DocumentKey) = mapOf(
        PARTITION_KEY to AttributeValue.fromS(id.partitionKey),
        SORT_KEY to AttributeValue.fromS(id.sortKey)
    )

    fun toDocumentKey(attributes: Map<String, AttributeValue>): DocumentKey = DocumentKey(
        partitionKey = attributes.getValue(PARTITION_KEY).s(),
        sortKey = attributes.getValue(SORT_KEY).s()
    )

    private companion object {
        private val jsonObject: Regex = Regex("^\\s*\\{")
    }
}
