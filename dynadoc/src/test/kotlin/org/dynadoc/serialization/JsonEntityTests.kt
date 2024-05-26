package org.dynadoc.serialization

import org.dynadoc.assertEntity
import org.dynadoc.core.DocumentKey
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

private val id: DocumentKey = DocumentKey("PK", "SK")

class JsonEntityTests {
    @Test
    fun modify_nonNull() {
        val entity = JsonEntity(id, "abc", 1)

        val result = entity.modify { "def" }

        assertEntity(result, id, "def", 1)
    }

    @Test
    fun modify_null() {
        val entity = JsonEntity(id, "abc", 1)

        val result = entity.modify { null }

        assertEntity(result, id, null, 1)
    }

    @Test
    fun createEntity_nonNull() {
        val result: JsonEntity<String> = createEntity("PK", "SK", "abc")

        assertEntity(result, id, "abc", 0)
    }

    @Test
    fun ifExists_nonNull() {
        val entity: JsonEntity<String?> = JsonEntity(id, "abc", 1)

        val result: JsonEntity<String>? = entity.ifExists()

        assertNotNull(result)
        assertEntity(result!!, id, "abc", 1)
    }

    @Test
    fun ifExists_null() {
        val entity: JsonEntity<String?> = JsonEntity(id, null, 1)

        val result = entity.ifExists()

        assertNull(result)
    }
}
