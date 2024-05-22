package org.dynadoc.core

import org.dynadoc.assertDocument
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId

private val id: DocumentKey = DocumentKey("PK", "SK")

class AttributeMapperTests {
    private val now = Instant.parse("2024-01-01T20:00:00.00Z")
    private val attributeMapper: AttributeMapper = AttributeMapper(
        Duration.ofSeconds(100),
        Clock.fixed(now, ZoneId.of("UTC"))
    )

    //region fromDocument

    @ParameterizedTest
    @CsvSource(
        value = [
            "{ \"key\": \"abc\" }        | S",
            "{ \"key\": 999 }            | N",
            "{ \"key\": true }           | BOOL",
            "{ \"key\": false }          | BOOL",
            "{ \"key\": null }           | NUL",
            "{ \"key\": [ 2, 3 ] }       | L",
            "{ \"key\": { \"sub\": 2 } } | M",
        ],
        delimiter = '|')
    fun fromDocument_validJson(json: String, type: AttributeValue.Type) {
        val attributes: Map<String, AttributeValue> = fromDocument(json)

        assertEquals(4, attributes.size)
        assertEquals("PK", attributes[PARTITION_KEY]?.s())
        assertEquals("SK", attributes[SORT_KEY]?.s())
        assertEquals(2, attributes[VERSION]?.n()?.toLong())
        assertEquals(type, attributes["key"]?.type())
    }

    @Test
    fun fromDocument_nullBody() {
        val attributes: Map<String, AttributeValue> = fromDocument(null)

        assertEquals(4, attributes.size)
        assertEquals("PK", attributes[PARTITION_KEY]?.s())
        assertEquals("SK", attributes[SORT_KEY]?.s())
        assertEquals(2, attributes[VERSION]?.n()?.toLong())
        assertEquals(1704139300, attributes[DELETED]?.n()?.toLong())
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "\"a\"",
        "10",
        "true",
        "false",
        "null",
        "[\"a\"]"
    ])
    fun fromDocument_invalidJson(json: String) {
        val exception = assertThrows<IllegalArgumentException> {
            fromDocument(json)
        }

        assertEquals("The document must be a JSON object.", exception.message)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        PARTITION_KEY,
        SORT_KEY,
        VERSION,
        DELETED
    ])
    fun fromDocument_invalidAttributes(attribute: String) {
        val exception = assertThrows<IllegalArgumentException> {
            fromDocument("{\"a\":1,\"$attribute\":2}")
        }

        assertEquals("The document cannot use the special attribute \"$attribute\".", exception.message)
    }

    //endregion fromDocument

    //region toDocument

    @ParameterizedTest
    @ValueSource(strings = [
        "{ \"key\": \"abc\" }",
        "{ \"key\": 999 }",
        "{ \"key\": true }",
        "{ \"key\": false }",
        "{ \"key\": null }",
        "{ \"key\": [ 2, 3 ] }",
        "{ \"key\": { \"sub\": 2 } }",
    ])
    fun toDocument_validJson(json: String) {
        val attributes: Map<String, AttributeValue> = fromDocument(json)

        val document: Document = toDocument(attributes)

        assertDocument(document, id, json, 2)
    }

    @Test
    fun toDocument_nullBody() {
        val attributes: Map<String, AttributeValue> = fromDocument(null)

        val document: Document = toDocument(attributes)

        assertDocument(document, id, null, 2)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        PARTITION_KEY,
        SORT_KEY,
        VERSION
    ])
    fun toDocument_missingAttribute(attribute: String) {
        val attributes: Map<String, AttributeValue> = fromDocument("{ \"key\": \"abc\" }")

        val exception = assertThrows<NoSuchElementException> {
            toDocument(attributes - attribute)
        }

        assertEquals("Key $attribute is missing in the map.", exception.message)
    }

    //endregion

    //region Helper Methods

    private fun createDocument(body: String?) = Document(id, body, 1)

    private fun fromDocument(body: String?) = attributeMapper.fromDocument(createDocument(body))

    private fun toDocument(attributes: Map<String, AttributeValue>) = attributeMapper.toDocument(attributes)

    //endregion
}
