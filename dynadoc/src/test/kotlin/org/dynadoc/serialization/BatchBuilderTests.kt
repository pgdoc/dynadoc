package org.dynadoc.serialization

import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.dynadoc.assertUpdateDocuments
import org.dynadoc.core.Document
import org.dynadoc.core.DocumentKey
import org.dynadoc.core.DocumentStore
import org.dynadoc.serialization.TestSerializer.jsonFor
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

private val ids = (0..9).map { i -> DocumentKey("document_$i", "KEY") }

class BatchBuilderTests {
    private val documentStore: DocumentStore = mockk {
        coEvery { updateDocuments(any(), any()) } returns Unit
    }
    private val batchBuilder: BatchBuilder = BatchBuilder(EntityStore(documentStore, TestSerializer))

    @Test
    fun submit_success() = runBlocking {
        check(ids[0], 0)
        modify(ids[1], 2)

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[1], jsonFor("abc"), 2)),
            checked = listOf(Document(ids[0], null, 0))
        )
    }

    //region check

    @Test
    fun check_afterCheckSuccess() = runBlocking {
        check(ids[0], 0)
        check(ids[0], 0)

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            checked = listOf(Document(ids[0], null, 0))
        )
    }

    @Test
    fun check_afterCheckError() {
        check(ids[0], 0)
        val exception = assertThrows<IllegalStateException> {
            check(ids[0], 1)
        }

        assertEquals("A different version of document ${ids[0]} is already being checked.", exception.message)
    }

    @Test
    fun check_afterModifySuccess() = runBlocking {
        modify(ids[0], 0)
        check(ids[0], 0)

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 0))
        )
    }

    @Test
    fun check_afterModifyError() {
        modify(ids[0], 0)
        val exception = assertThrows<IllegalStateException> {
            check(ids[0], 1)
        }

        assertEquals("A different version of document ${ids[0]} is already being modified.", exception.message)
    }

    @Test
    fun check_multipleError() = runBlocking {
        modify(ids[0], 0)

        assertThrows<IllegalStateException> {
            batchBuilder.check(
                JsonEntity(ids[1], "def", 10),
                JsonEntity(ids[0], "def", 11)
            )
        }

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 0))
        )
    }

    //endregion

    //region modify

    @Test
    fun modify_afterCheckSuccess() = runBlocking {
        check(ids[0], 0)
        modify(ids[0], 0)

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 0))
        )
    }

    @Test
    fun modify_afterCheckError() {
        check(ids[0], 0)
        val exception = assertThrows<IllegalStateException> {
            modify(ids[0], 1)
        }

        assertEquals("A different version of document ${ids[0]} is already being checked.", exception.message)
    }

    @Test
    fun modify_afterModifyError() {
        modify(ids[0], 0)
        val exception = assertThrows<IllegalStateException> {
            modify(ids[0], 0)
        }

        assertEquals("Document ${ids[0]} is already being modified.", exception.message)
    }

    @Test
    fun modify_multipleError() = runBlocking {
        modify(ids[0], 0)

        assertThrows<IllegalStateException> {
            batchBuilder.modify(
                JsonEntity(ids[1], "def", 10),
                JsonEntity(ids[0], "def", 11)
            )
        }

        batchBuilder.submit()

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 0))
        )
    }

    //endregion

    //region Helper Methods

    private fun modify(id: DocumentKey, version: Long) =
        batchBuilder.modify(JsonEntity(id, "abc", version))

    private fun check(id: DocumentKey, version: Long) =
        batchBuilder.check(JsonEntity(id, "def", version))

    //endregion
}
