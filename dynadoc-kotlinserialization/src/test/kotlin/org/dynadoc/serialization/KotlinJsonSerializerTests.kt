package org.dynadoc.serialization

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.dynadoc.core.Document
import org.dynadoc.core.DocumentKey
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import kotlin.reflect.typeOf

class KotlinJsonSerializerTests {
    private val serializer: KotlinJsonSerializer = KotlinJsonSerializer(Json)

    @Test
    fun serialize_string() {
        val result: String = serializer.serialize(testObject)

        JSONAssert.assertEquals(testJson, result, true)
    }

    @Test
    fun deserialize_string() {
        val result: TestClass = serializer.deserialize(testJson, typeOf<TestClass>())

        assertEquals(testObject, result)
    }

    @Test
    fun toDocument_document() {
        val document: JsonEntity<TestClass> = JsonEntity(
            id = DocumentKey("PK", "SK"),
            entity = testObject,
            version = 1
        )

        val result: Document = serializer.toDocument(document)

        JSONAssert.assertEquals(testJson, result.body, true)
    }

    @Test
    fun fromDocument_document() {
        val document = Document(
            id = DocumentKey("PK", "SK"),
            body = testJson,
            version = 1
        )

        val result: JsonEntity<TestClass?> = serializer.fromDocument(document)

        assertEquals(testObject, result.entity)
    }
}

@Serializable
private data class TestClass(
    val stringKey: String,
    val numberKey: Int,
    val booleanKey: Boolean,
    val listKey: List<Long>,
    val mapKey: Map<String, Long>
)

private val testObject = TestClass(
    stringKey = "value",
    numberKey = 999,
    booleanKey = true,
    listKey = listOf(10, 20, 30),
    mapKey = mapOf(
        "a" to 1,
        "b" to 2
    )
)

private val testJson = "{\"stringKey\": \"value\", \"numberKey\": 999, \"booleanKey\": true, \"listKey\": [10, 20, 30], \"mapKey\": { \"a\": 1, \"b\": 2 }}"
