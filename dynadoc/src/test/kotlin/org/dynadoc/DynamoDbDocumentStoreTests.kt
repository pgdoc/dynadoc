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