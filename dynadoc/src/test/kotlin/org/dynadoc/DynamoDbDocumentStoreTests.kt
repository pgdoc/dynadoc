package org.dynadoc

import aws.sdk.kotlin.runtime.auth.credentials.StaticCredentialsProvider
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.smithy.kotlin.runtime.net.url.Url
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.util.*
import java.util.stream.Stream

class DynamoDbDocumentStoreTests {
    private lateinit var ids: List<DocumentKey>
    private lateinit var store: DynamoDbDocumentStore

    //region updateDocuments

    @ParameterizedTest
    @MethodSource("org.dynadoc.MethodSources#updateDocuments_oneArgument")
    fun updateDocuments_emptyToValue(to: String?) = runBlocking {
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.MethodSources#updateDocuments_twoArguments")
    fun updateDocuments_valueToValue(from: String?, to: String?) = runBlocking {
        updateDocument(from, 0)
        updateDocument(to, 1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 2)
    }

    @Test
    fun updateDocuments_emptyToCheck() = runBlocking {
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.MethodSources#updateDocuments_oneArgument")
    fun updateDocuments_valueToCheck(from: String?) = runBlocking {
        updateDocument(from, 0)
        checkDocument(1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], from, 1)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.MethodSources#updateDocuments_oneArgument")
    fun updateDocuments_checkToValue(to: String?) = runBlocking {
        checkDocument(0)
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @Test
    fun updateDocuments_checkToCheck() = runBlocking {
        checkDocument(0)
        checkDocument(0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentDoesNotExist(checkOnly: Boolean) = runBlocking {
        val exception = assertThrows(
            UpdateConflictException::class.java,
            fun() = runBlocking {
                if (checkOnly) {
                    checkDocument(10)
                } else {
                    updateDocument("{\"abc\":\"def\"}", 10)
                }
            })

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], null, 0)
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictWrongVersion(checkOnly: Boolean) = runBlocking {
        updateDocument("{\"abc\":\"def\"}", 0)

        val exception = assertThrows(
            UpdateConflictException::class.java,
            fun() = runBlocking {
                if (checkOnly) {
                    checkDocument(10)
                } else {
                    updateDocument("{\"abc\":\"def\"}", 10)
                }
            })

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], "{\"abc\":\"def\"}", 1);
        assertEquals(ids[0], exception.id)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_conflictDocumentAlreadyExists(checkOnly: Boolean) = runBlocking {
        updateDocument("{\"abc\":\"def\"}", 0)

        val exception = assertThrows(
            UpdateConflictException::class.java,
            fun() = runBlocking {
                if (checkOnly) {
                    checkDocument(0)
                } else {
                    updateDocument("{\"abc\":\"def\"}", 0)
                }
            })

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], "{\"abc\":\"def\"}", 1);
        assertEquals(ids[0], exception.id)
    }

    @Test
    fun updateDocuments_multipleDocumentsSuccess() = runBlocking {
        updateDocument(ids[0], "{\"abc\":\"def\"}", 0)
        updateDocument(ids[1], "{\"ghi\":\"jkl\"}", 0)

        store.updateDocuments(
            listOf(
                Document(ids[0], "{\"v\":\"1\"}", 1),
                Document(ids[2], "{\"v\":\"2\"}", 0)
            ),
            listOf(
                Document(ids[1], "{\"v\":\"3\"}", 1),
                Document(ids[3], "{\"v\":\"4\"}", 0)
            )
        )

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])
        val document3 = store.getDocument(ids[2])
        val document4 = store.getDocument(ids[3])

        assertDocument(document1, ids[0], "{\"v\":\"1\"}", 2)
        assertDocument(document2, ids[1], "{\"ghi\":\"jkl\"}", 1)
        assertDocument(document3, ids[2], "{\"v\":\"2\"}", 1)
        assertDocument(document4, ids[3], null, 0)
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun updateDocuments_multipleDocumentsConflict(checkOnly: Boolean) = runBlocking {
        updateDocument(ids[0], "{\"abc\":\"def\"}", 0)

        val exception = assertThrows(
            UpdateConflictException::class.java,
            fun() = runBlocking {
                if (checkOnly) {
                    store.updateDocuments(
                        listOf(Document(ids[0], "{\"ghi\":\"jkl\"}", 1)),
                        listOf(Document(ids[1], "{\"mno\":\"pqr\"}", 10))
                    )
                } else {
                    store.updateDocuments(
                        Document(ids[0], "{\"ghi\":\"jkl\"}", 1),
                        Document(ids[1], "{\"mno\":\"pqr\"}", 10)
                    )
                }
            })

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], "{\"abc\":\"def\"}", 1)
        assertDocument(document2, ids[1], null, 0)
        assertEquals(ids[1], exception.id)
    }

    //endregion

    //region Helper Methods

    private suspend fun updateDocument(body: String?, version: Long) {
        updateDocument(ids[0], body, version)
    }

    private suspend fun updateDocument(id: DocumentKey, body: String?, version: Long) {
        store.updateDocuments(Document(id, body, version))
    }

    private suspend fun checkDocument(version: Long) {
        store.updateDocuments(
            listOf(),
            listOf(Document(ids[0], "{\"ignored\":\"ignored\"}", version))
        )
    }

    private fun assertDocument(document: Document, id: DocumentKey, body: String?, version: Long) {
        assertEquals(id, document.id)

        if (body == null) {
            assertNull(document.body)
        } else {
            assertNotNull(document.body)
            assertEquals(body, document.body)
        }

        assertEquals(version, document.version)
    }

    //endregion

    //region Setup

    private companion object Setup {
        lateinit var client: DynamoDbClient

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            client = DynamoDbClient.builder()
                .apply {
                    this.config.apply {
                        this.endpointUrl = Url.parse("http://localhost:8000")
                        this.credentialsProvider = StaticCredentialsProvider {
                            this.accessKeyId = "N/A"
                            this.secretAccessKey = "N/A"
                        }
                        this.region = "eu-west-1"
                    }
                }
                .build()
        }
    }

    @BeforeEach
    fun testSetup() {
        store = DynamoDbDocumentStore(
            client = client,
            tableName = "tests"
        )
        ids = (0..10).map { i -> DocumentKey("${UUID.randomUUID()}_$i", "0000") }
    }

    //endregion
}

object MethodSources {
    @JvmStatic
    fun updateDocuments_oneArgument(): Stream<String?> {
        return Stream.of(
            "{\"abc\":\"def\"}",
            null
        )
    }

    @JvmStatic
    fun updateDocuments_twoArguments(): Stream<Arguments> {
        return Stream.of(
            Arguments.of("{\"abc\":\"def\"}", "{\"ghi\":\"jkl\"}"),
            Arguments.of(null, "{\"ghi\":\"jkl\"}"),
            Arguments.of("{\"abc\":\"def\"}", null),
            Arguments.of(null, null)
        )
    }
}