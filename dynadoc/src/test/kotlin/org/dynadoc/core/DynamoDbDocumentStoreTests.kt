package org.dynadoc.core

import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
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
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException
import java.net.URI
import java.util.*
import java.util.stream.Stream
import kotlin.random.asKotlinRandom

@Testcontainers
class DynamoDbDocumentStoreTests {
    private val store: DynamoDbDocumentStore = DynamoDbDocumentStore(client, "tests")

    private val partitionKey = UUID.randomUUID().toString()
    private val ids: List<DocumentKey> = (0..10).map { i -> DocumentKey("${partitionKey}_$i", "0000") }
    private val longJson: String = "{\"key\":\"${"a".repeat(1024 * 1024)}\"}"
    private val string100Kb: String = "a".repeat(100 * 1024)
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

    //region query

    @Test
    fun query_filterSortKey() = runBlocking {
        val documents = (0..9).map { i ->
            Document(DocumentKey(partitionKey, "ABC0$i"), "{\"a\":$i}", 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.query {
                keyConditionExpression("partition_key = :pk AND sort_key BETWEEN :start AND :end")
                expressionAttributeValues(mapOf(
                    ":pk" to AttributeValue.fromS(partitionKey),
                    ":start" to AttributeValue.fromS("ABC03"),
                    ":end" to AttributeValue.fromS("ABC05")
                ))
            }.toList()

        assertDocuments(result, documents.slice(3..5))
    }

    @Test
    fun query_filterNonKey() = runBlocking {
        val documents = (0..9).map { i ->
            Document(DocumentKey(partitionKey, "ABC0$i"), "{\"a\":$i}", 0)
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.query {
                keyConditionExpression("partition_key = :pk")
                filterExpression("a > :start")
                expressionAttributeValues(mapOf(
                    ":pk" to AttributeValue.fromS(partitionKey),
                    ":start" to AttributeValue.fromN("4"),
                ))
            }.toList()

        assertDocuments(result, documents.slice(5..9))
    }

    @Test
    fun query_pagination() = runBlocking {
        val documents = (100..199).map { i ->
            Document(DocumentKey(partitionKey, "ABC0$i"), "{\"b\":\"$string100Kb\"}", 0)
        }
        documents.forEach { document -> store.updateDocuments(document) }

        val result =
            store.query {
                keyConditionExpression("partition_key = :pk AND sort_key BETWEEN :start AND :end")
                expressionAttributeValues(mapOf(
                    ":pk" to AttributeValue.fromS(partitionKey),
                    ":start" to AttributeValue.fromS("ABC0120"),
                    ":end" to AttributeValue.fromS("ABC0180")
                ))
            }.toList()

        assertDocuments(result, documents.slice(20..80))
    }

    //endregion

    //region scan

    @Test
    fun scan_filterAttribute() = runBlocking {
        val documents = (0..9).map { i ->
            Document(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = "{\"b\":\"value $i\"}",
                version = 0
            )
        }
        store.updateDocuments(*documents.toTypedArray())

        val result =
            store.scan {
                filterExpression("b BETWEEN :start AND :end")
                expressionAttributeValues(mapOf(
                    ":start" to AttributeValue.fromS("val"),
                    ":end" to AttributeValue.fromS("value 4")
                ))
            }.toList()

        assertDocuments(result.sortedBy { it.id.partitionKey }, documents.slice(0..4))
    }

    @Test
    fun scan_pagination() = runBlocking {
        val documents = (100..199).map { i ->
            Document(
                id = DocumentKey("${partitionKey}_$i", "0000"),
                body = "{\"b\":\"$string100Kb\"}",
                version = 0
            )
        }
        documents.forEach { document -> store.updateDocuments(document) }

        val result =
            store.scan {
                filterExpression("partition_key BETWEEN :start AND :end")
                expressionAttributeValues(mapOf(
                    ":start" to AttributeValue.fromS("${partitionKey}_120"),
                    ":end" to AttributeValue.fromS("${partitionKey}_180")
                ))
            }.toList()

        assertDocuments(result.sortedBy { it.id.partitionKey }, documents.slice(20..80))
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

    private fun assertDocuments(actual: List<Document>, expected: List<Document>) {
        assertEquals(expected.size, actual.size)

        repeat(actual.size) { i ->
            assertDocument(actual[i], expected[i].id, expected[i].body, 1)
        }
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
        } + "{ }"

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