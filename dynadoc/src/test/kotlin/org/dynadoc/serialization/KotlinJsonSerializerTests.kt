package org.dynadoc.serialization

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import org.skyscreamer.jsonassert.JSONAssert
import kotlin.test.assertEquals

class KotlinJsonSerializerTests {
    private val serializer: KotlinJsonSerializer = KotlinJsonSerializer(Json)

    @Test
    fun serialize_success() {
        val result = serializer.serialize(testObject)

        JSONAssert.assertEquals(testJson, result, true)
    }

    @Test
    fun deserialize_success() {
        val result = serializer.deserialize(testJson, TestClass::class.java)

        assertEquals(testObject, result)
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
