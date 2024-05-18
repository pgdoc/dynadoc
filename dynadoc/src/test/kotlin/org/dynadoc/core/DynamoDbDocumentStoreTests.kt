package org.dynadoc.core

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import org.skyscreamer.jsonassert.JSONAssert
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import java.net.URI
import java.util.*
import java.util.stream.Stream
import kotlin.random.asKotlinRandom

@Testcontainers
class DynamoDbDocumentStoreTests {
    private lateinit var ids: List<DocumentKey>
    private lateinit var store: DynamoDbDocumentStore
    private var longJson: String = "{\"key\":\"${"a".repeat(1024 * 1024)}\"}"

    //region updateDocuments

    @ParameterizedTest
    @MethodSource("org.dynadoc.core.MethodSources#updateDocuments_oneArgument")
    fun updateDocuments_emptyToValue(to: String?) = runBlocking {
        updateDocument(to, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], to, 1)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.core.MethodSources#updateDocuments_twoArguments")
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
    @MethodSource("org.dynadoc.core.MethodSources#updateDocuments_oneArgument")
    fun updateDocuments_valueToCheck(from: String?) = runBlocking {
        updateDocument(from, 0)
        checkDocument(1)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], from, 1)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.core.MethodSources#updateDocuments_oneArgument")
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

    @Test
    fun updateDocuments_genericError() = runBlocking {
        assertThrows(
            DynamoDbException::class.java,
            fun() = runBlocking {
                updateDocument(longJson, 0)
            })

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

    @Test
    fun updateDocuments_multipleDocumentsGenericError() = runBlocking {
        updateDocument(ids[0], "{\"abc\":\"def\"}", 0)

        assertThrows(
            DynamoDbException::class.java,
            fun() = runBlocking {
                store.updateDocuments(
                    Document(ids[0], "{\"ghi\":\"jkl\"}", 1),
                    Document(ids[1], longJson, 0)
                )
            })

        val document1 = store.getDocument(ids[0])
        val document2 = store.getDocument(ids[1])

        assertDocument(document1, ids[0], "{\"abc\":\"def\"}", 1)
        assertDocument(document2, ids[1], null, 0)
    }

    //endregion

    //region getDocuments

    @Test
    fun getDocuments_singleDocument() = runBlocking {
        updateDocument("{\"abc\":\"def\"}", 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0])).toList()

        assertEquals(1, documents.size)
        assertDocument(documents[0], ids[0], "{\"abc\":\"def\"}", 1)
    }

    @Test
    fun getDocuments_multipleDocuments() = runBlocking {
        updateDocument(ids[0], "{\"abc\":\"def\"}", 0)
        updateDocument(ids[1], "{\"ghi\":\"jkl\"}", 0)

        val documents: List<Document> = store.getDocuments(listOf(ids[0], ids[2], ids[0], ids[1])).toList()

        assertEquals(4, documents.size)
        assertDocument(documents[0], ids[0], "{\"abc\":\"def\"}", 1)
        assertDocument(documents[1], ids[2], null, 0)
        assertDocument(documents[2], ids[0], "{\"abc\":\"def\"}", 1)
        assertDocument(documents[3], ids[1], "{\"ghi\":\"jkl\"}", 1)
    }

    @Test
    fun getDocuments_noDocument() = runBlocking {
        val documents: List<Document> = store.getDocuments(listOf()).toList()

        assertEquals(0, documents.size)
    }

    @ParameterizedTest
    @MethodSource("org.dynadoc.core.MethodSources#getDocuments_jsonDeserialization")
    fun getDocuments_jsonDeserialization(json: String) = runBlocking {
        updateDocument(ids[0], json, 0)

        val document = store.getDocument(ids[0])

        assertDocument(document, ids[0], json, 1)
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
            JSONAssert.assertEquals(body, document.body, true)
        }

        assertEquals(version, document.version)
    }

    //endregion

    //region Setup

    private companion object Setup {
        lateinit var client: DynamoDbAsyncClient
        var port: Int = Random().asKotlinRandom().nextInt(10000, 32000)

        @JvmStatic
        @Container
        private val container = GenericContainer("amazon/dynamodb-local:latest").apply {
            portBindings = listOf("$port:8000")
            setCommand("-jar DynamoDBLocal.jar -sharedDb -inMemory")
            workingDirectory = "/home/dynamodblocal"
            waitingFor(Wait
                .forHttp("/")
                .forPort(8000)
                .forStatusCode(400))
        }

        @BeforeAll
        @JvmStatic
        fun globalSetup() {
            client = DynamoDbAsyncClient.builder()
                .endpointOverride(URI.create("http://localhost:$port"))
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("NONE", "NONE")))
                .region(Region.EU_WEST_1)
                .build()

            require(container.isRunning()) { container.logs }
            runBlocking {
                DynamoDbDocumentStore(client, "tests").createTable()
            }
        }
    }

    @BeforeEach
    fun testSetup() {
        store = DynamoDbDocumentStore(client, "tests")
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

    @JvmStatic
    fun getDocuments_jsonDeserialization(): Stream<String> {
        val scalars: List<String> = listOf(
            "1234567890.0987654321",
            "\"text\"",
            "true",
            "false",
            "null"
        )

        val firstLevel: List<String> = scalars.map {
            "{ \"a\": $it }"
        }

        val nestedObjects = firstLevel.map {
            "{ \"b\": $it }"
        }

        val arrayOfObjects = (scalars + firstLevel).map {
            val repeat = "$it, $it, $it"
            "{ \"b\": [ $repeat ] }"
        }

        val mixedArray = "{ \"c\": [ ${(scalars + firstLevel).joinToString()} ] }"

        return (firstLevel + nestedObjects + arrayOfObjects + mixedArray).stream()
    }
}