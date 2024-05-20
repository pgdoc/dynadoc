package org.dynadoc.core

import org.dynadoc.core.AttributeMapper.DELETED
import org.dynadoc.core.AttributeMapper.PARTITION_KEY
import org.dynadoc.core.AttributeMapper.SORT_KEY
import org.dynadoc.core.AttributeMapper.VERSION
import org.dynadoc.core.AttributeMapper.fromDocument
import org.dynadoc.core.AttributeMapper.toDocument
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import org.skyscreamer.jsonassert.JSONAssert
import software.amazon.awssdk.services.dynamodb.model.AttributeValue

class AttributeMapperTests {
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
        val attributes: Map<String, AttributeValue> = fromDocument(createDocument(json))

        assertEquals(5, attributes.size)
        assertEquals("PK", attributes[PARTITION_KEY]?.s())
        assertEquals("SK", attributes[SORT_KEY]?.s())
        assertEquals(2, attributes[VERSION]?.n()?.toLong())
        assertEquals(false, attributes[DELETED]?.bool())
        assertEquals(type, attributes["key"]?.type())
    }

    @Test
    fun fromDocument_nullBody() {
        val attributes: Map<String, AttributeValue> = fromDocument(createDocument(null))

        assertEquals(4, attributes.size)
        assertEquals("PK", attributes[PARTITION_KEY]?.s())
        assertEquals("SK", attributes[SORT_KEY]?.s())
        assertEquals(2, attributes[VERSION]?.n()?.toLong())
        assertEquals(true, attributes[DELETED]?.bool())
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
        val exception = Assertions.assertThrows(IllegalArgumentException::class.java) {
            fromDocument(createDocument(json))
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
        val exception = Assertions.assertThrows(IllegalArgumentException::class.java) {
            fromDocument(createDocument("{\"a\":1,\"$attribute\":2}"))
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
        val attributes: Map<String, AttributeValue> = fromDocument(createDocument(json))

        val document: Document = toDocument(attributes)

        assertEquals(DocumentKey("PK", "SK"), document.id)
        JSONAssert.assertEquals(json, document.body, true)
        assertEquals(2, document.version)
    }

    @Test
    fun toDocument_nullBody() {
        val attributes: Map<String, AttributeValue> = fromDocument(createDocument(null))

        val document: Document = toDocument(attributes)

        assertEquals(DocumentKey("PK", "SK"), document.id)
        assertNull(null, document.body)
        assertEquals(2, document.version)
    }

    //endregion

    //region Helper Methods

    private fun createDocument(body: String?) = Document(
        id = DocumentKey("PK", "SK"),
        body = body,
        version = 1
    )

    //endregion
}
