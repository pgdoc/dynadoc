package org.dynadoc;

import io.mockk.coVerify
import org.dynadoc.core.Document
import org.dynadoc.core.DocumentKey
import org.dynadoc.core.DocumentStore
import org.dynadoc.serialization.JsonEntity
import org.junit.jupiter.api.Assertions.*
import org.skyscreamer.jsonassert.JSONAssert
import kotlin.test.assertContentEquals

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

fun DocumentStore.assertUpdateDocuments(checked: List<Document> = emptyList(), updated: List<Document> = emptyList()) =
    coVerify(exactly = 1) {
        this@assertUpdateDocuments.updateDocuments(
            updatedDocuments = coWithArg {
                assertContentEquals(updated.toList(), it.toList())
            },
            checkedDocuments = coWithArg {
                assertContentEquals(checked.toList(), it.toList())
            }
        )
    }
