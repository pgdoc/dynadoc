package org.dynadoc.serialization

import kotlinx.coroutines.flow.toList
import org.dynadoc.core.DocumentKey
import org.dynadoc.core.DocumentStore

/**
 * Represents a service object used to retrieve and modify documents represented as [JsonEntity] objects.
 */
class EntityStore(
    private val documentStore: DocumentStore,
    private val jsonSerializer: JsonSerializer
) {
    /**
     * Updates atomically the body of multiple documents represented as [JsonEntity] objects.
     */
    suspend fun updateEntities(
        updatedDocuments: Iterable<JsonEntity<Any?>> = emptyList(),
        checkedDocuments: Iterable<JsonEntity<Any?>> = emptyList()
    ) {
        documentStore.updateDocuments(
            updatedDocuments = updatedDocuments
                .map(jsonSerializer::toDocument),
            checkedDocuments = checkedDocuments
                .map { entity -> entity.copy(entity = null) }
                .map(jsonSerializer::toDocument)
        )
    }

    /**
     * Retrieves a document given its ID, represented as a [JsonEntity] object.
     */
    suspend fun <T : Any> getEntities(ids: Iterable<DocumentKey>, clazz: Class<T>): List<JsonEntity<T?>> {
        val result = documentStore.getDocuments(ids).toList()
        return result.map { jsonSerializer.fromDocument(it, clazz) }
    }
}


suspend inline fun <reified T : Any> EntityStore.getEntities(ids: Iterable<DocumentKey>): List<JsonEntity<T?>> =
    getEntities(ids, T::class.java)

suspend inline fun <reified T : Any> EntityStore.getEntity(id: DocumentKey): JsonEntity<T?> =
    getEntities(listOf(id), T::class.java)[0]

suspend fun EntityStore.updateEntities(vararg updatedEntities: JsonEntity<Any?>) =
    updateEntities(
        updatedDocuments = updatedEntities.asIterable(),
        checkedDocuments = emptyList()
    )
