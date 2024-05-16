package org.dynadoc

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.dynamodb.paginators.queryPaginated
import kotlinx.coroutines.flow.*
import org.dynadoc.DynamoDbDocument.conditionalCheckFailureReasons

class DynamoDbDocumentStore(
    private val client: DynamoDbClient,
    private val tableName: String
) : DocumentStore {

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        fun conditionExpression(version: Long) =
            if (version == 0L) {
                "attribute_not_exists(partition_key)"
            } else {
                "version = :version"
            }

        fun expressionAttributeValues(version: Long) =
            if (version == 0L) {
                null
            } else {
                mapOf(":version" to AttributeValue.N(version.toString()))
            }

        val updates: Sequence<TransactWriteItem> = updatedDocuments.asSequence().map { document ->
            TransactWriteItem {
                put = Put {
                    tableName = this@DynamoDbDocumentStore.tableName
                    item = buildMap {
                        put(Table.PARTITION_KEY, AttributeValue.S(document.id.partitionKey))
                        put(Table.SORT_KEY, AttributeValue.S(document.id.sortKey))
                        put(Table.VERSION, AttributeValue.N((document.version + 1).toString()))

                        document.body?.let {
                            put(Table.BODY, AttributeValue.S(it))
                        }
                    }
                    conditionExpression = conditionExpression(document.version)
                    expressionAttributeValues = expressionAttributeValues(document.version)
                    returnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.AllOld
                }
            }
        }

        val checks: Sequence<TransactWriteItem> = checkedDocuments.asSequence().map { document ->
            TransactWriteItem {
                conditionCheck = ConditionCheck {
                    tableName = this@DynamoDbDocumentStore.tableName
                    key = Table.getKeyAttributes(document.id)
                    conditionExpression = conditionExpression(document.version)
                    expressionAttributeValues = expressionAttributeValues(document.version)
                    returnValuesOnConditionCheckFailure = ReturnValuesOnConditionCheckFailure.AllOld
                }
            }
        }

        val elements = (updates + checks).toList();

        try {
            val write: TransactWriteItemsRequest = TransactWriteItemsRequest {
                transactItems = elements
            }

            client.transactWriteItems(write)

        } catch (exception: TransactionCanceledException) {
            val checkFailureIndex: Int = exception.cancellationReasons
                ?. indexOfFirst { it.code == "ConditionalCheckFailed" }
                ?: -1

            if (checkFailureIndex >= 0) {
                val failure: TransactWriteItem = elements[checkFailureIndex]
                val attributes: Map<String, AttributeValue> = failure.put?.item
                    ?: failure.conditionCheck?.key!!

                throw UpdateConflictException(DynamoDbDocument.parseKey(attributes))
            } else {
                throw exception
            }
        }
    }

    override fun getDocuments(ids: Iterable<DocumentKey>): Flow<Document> {
        val idList: List<DocumentKey> = ids.toList()

        if (idList.isEmpty()) {
            return flowOf()
        }

        val request: BatchGetItemRequest = BatchGetItemRequest {
            requestItems = mapOf(
                tableName to KeysAndAttributes {
                    keys = idList.distinct().map(Table::getKeyAttributes)
                }
            )

            returnConsumedCapacity = ReturnConsumedCapacity.Total
        }

        val result: Flow<List<Document>> = flow {
            val responses = client.batchGetItem(request).responses

            if (responses != null) {
                val documents: Map<DocumentKey, Document> = responses.values
                    .flatten()
                    .map(DynamoDbDocument::parse)
                    .associateBy { it.id }

                val result = idList
                    .map { id ->
                        documents[id] ?: Document(id, null, 0)
                    }

                flowOf(result).collect(this)
            }
        }

        return result.flatMapConcat { it.asFlow() }
    }

    fun runQuery(query: QueryRequest): Flow<Document> {
        return client.queryPaginated(query)
            .mapNotNull { response -> response.items }
            .flatMapConcat { items -> items.asFlow() }
            .map(DynamoDbDocument::parse)
    }

    suspend fun createTable(configure: CreateTableRequest.Builder.() -> Unit = { }) {
        val request: CreateTableRequest = CreateTableRequest {
            tableName = this@DynamoDbDocumentStore.tableName
            keySchema = listOf(
                KeySchemaElement {
                    attributeName = Table.PARTITION_KEY
                    keyType = KeyType.Hash
                },
                KeySchemaElement {
                    attributeName = Table.SORT_KEY
                    keyType = KeyType.Range
                }
            )
            attributeDefinitions = listOf(
                AttributeDefinition {
                    attributeName = Table.PARTITION_KEY
                    attributeType = ScalarAttributeType.S
                },
                AttributeDefinition {
                    attributeName = Table.SORT_KEY
                    attributeType = ScalarAttributeType.S
                }
            )
            billingMode = BillingMode.PayPerRequest

            configure()
        }

        client.createTable(request);
    }
}

private object DynamoDbDocument {
    val conditionalCheckFailureReasons = setOf("ConditionalCheckFailed", "None")

    fun parse(attributes: Map<String, AttributeValue>): Document = Document(
        id = parseKey(attributes),
        body = attributes[Table.BODY]?.asS(),
        version = attributes[Table.VERSION]!!.asN().toLong()
    )

    fun parseKey(attributes: Map<String, AttributeValue>): DocumentKey = DocumentKey(
        partitionKey = attributes[Table.PARTITION_KEY]!!.asS(),
        sortKey = attributes[Table.SORT_KEY]!!.asS()
    )
}

private object Table {
    const val PARTITION_KEY = "partition_key"
    const val SORT_KEY = "sort_key"
    const val BODY = "body"
    const val VERSION = "version"

    fun getKeyAttributes(id: DocumentKey) = mapOf(
        PARTITION_KEY to AttributeValue.S(id.partitionKey),
        SORT_KEY to AttributeValue.S(id.sortKey)
    )
}