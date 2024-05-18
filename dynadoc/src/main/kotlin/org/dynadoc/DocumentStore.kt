package org.dynadoc

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import java.util.*

interface DocumentStore {
    @Throws(UpdateConflictException::class)
    suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>)

    fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document>
}

class UpdateConflictException(
    val id: DocumentKey
): RuntimeException("The object $id has been modified.")


@Throws(UpdateConflictException::class)
suspend fun DocumentStore.updateDocuments(vararg documents: Document) =
    updateDocuments(documents.toList(), emptyList())

suspend fun DocumentStore.getDocument(id: DocumentKey): Document =
    getDocuments(listOf(id)).first()
