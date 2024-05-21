package org.dynadoc.serialization

import io.mockk.*
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.dynadoc.assertEntity
import org.dynadoc.assertUpdateDocuments
import org.dynadoc.core.Document
import org.dynadoc.core.DocumentKey
import org.dynadoc.core.DocumentStore
import org.dynadoc.serialization.TestSerializer.jsonFor
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import kotlin.test.assertEquals

private val ids = (0..9).map { i -> DocumentKey("document_$i", "KEY") }
private val idsNull = (0..9).map { i -> DocumentKey("document_$i", "NULL") }

class EntityStoreTests {
    private val documentStore: DocumentStore = mockk {
        coEvery { updateDocuments(any(), any()) } returns Unit
        coEvery { getDocuments(any()) } answers {
            firstArg<Iterable<DocumentKey>>()
                .mapIndexed { index, key ->
                    Document(
                        id = key,
                        body = if (key.sortKey == "NULL") { null } else { jsonFor(key.partitionKey) },
                        version = index + 1L
                    )
                }
                .asFlow()
        }
    }
    private val store: EntityStore = EntityStore(documentStore, TestSerializer)

    //region updateEntities

    @Test
    fun updateEntities_updateToValue() = runBlocking {
        val document = JsonEntity(ids[0], "abc", 1)

        store.updateEntities(document)

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], jsonFor("abc"), 1))
        )
    }

    @Test
    fun updateEntities_updateToNull() = runBlocking {
        val document = JsonEntity(ids[0], null, 1)

        store.updateEntities(document)

        documentStore.assertUpdateDocuments(
            updated = listOf(Document(ids[0], null, 1))
        )
    }

    @Test
    fun updateEntities_check() = runBlocking {
        val document = JsonEntity(ids[0], 5.5f, 1)

        store.updateEntities(emptyList(), listOf(document))

        documentStore.assertUpdateDocuments(
            checked = listOf(Document(ids[0], null, 1))
        )
    }

    @Test
    fun updateEntities_updateAndCheck() = runBlocking {
        store.updateEntities(
            listOf(
                JsonEntity(ids[0], "abc", 1),
                JsonEntity(ids[1], null, 2)
            ),
            listOf(
                JsonEntity(ids[2], 5.5f, 3),
                JsonEntity(ids[3], BigDecimal("21"), 4)
            )
        )

        documentStore.assertUpdateDocuments(
            updated = listOf(
                Document(ids[0], jsonFor("abc"), 1),
                Document(ids[1], null, 2)
            ),
            checked = listOf(
                Document(ids[2], null, 3),
                Document(ids[3], null, 4)
            )
        )
    }

    //endregion

    //region getEntities

    @Test
    fun getEntities_multiple() = runBlocking {
        val result: List<JsonEntity<String?>> = store.getEntities(listOf(ids[0], idsNull[1], ids[2]))

        assertEquals(3, result.size)
        assertEntity(result[0], ids[0], "document_0", 1)
        assertEntity(result[1], idsNull[1], null, 2)
        assertEntity(result[2], ids[2], "document_2", 3)
    }

    @Test
    fun getEntity_notNull() = runBlocking {
        val result: JsonEntity<String?> = store.getEntity(ids[0])

        assertEntity(result, ids[0], "document_0", 1)
    }

    @Test
    fun getEntity_null() = runBlocking {
        val result: JsonEntity<String?> = store.getEntity(idsNull[0])

        assertEntity(result, idsNull[0], null, 1)
    }

    //endregion
}
