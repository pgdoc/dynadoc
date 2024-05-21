package org.dynadoc;

import org.dynadoc.core.Document
import org.dynadoc.core.DocumentKey
import org.dynadoc.serialization.JsonEntity
import org.junit.jupiter.api.Assertions.*
import org.skyscreamer.jsonassert.JSONAssert

fun assertDocument(document: Document, id: DocumentKey, body: String?, version: Long) {
    assertEquals(id, document.id)

    if (body == null) {
        assertNull(document.body)
    } else {
        assertNotNull(document.body)
        JSONAssert.assertEquals(body, document.body, true)
    }

    assertEquals(version, document.version)
}

fun <T> assertEntity(document: JsonEntity<T>, id: DocumentKey, entity: T?, version: Long) {
    assertEquals(id, document.id)

    if (entity == null) {
        assertNull(document.entity)
    } else {
        assertNotNull(document.entity)
        assertEquals(entity, document.entity)
    }

    assertEquals(version, document.version)
}
