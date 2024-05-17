package org.dynadoc

import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.model.*
import aws.sdk.kotlin.services.dynamodb.paginators.queryPaginated
import kotlinx.coroutines.flow.*
import org.dynadoc.AttributeMapper.PARTITION_KEY
import org.dynadoc.AttributeMapper.SORT_KEY

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
                    item = AttributeMapper.fromDocument(document)
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
                    key = AttributeMapper.getKeyAttributes(document.id)
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

                throw UpdateConflictException(AttributeMapper.parseKey(attributes))
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
                    keys = idList.distinct().map(AttributeMapper::getKeyAttributes)
                    consistentRead = true
                }
            )
        }

        val result: Flow<List<Document>> = flow {
            val responses = client.batchGetItem(request).responses

            if (responses != null) {
                val documents: Map<DocumentKey, Document> = responses.values
                    .flatten()
                    .map(AttributeMapper::toDocument)
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

//    fun runQuery(query: QueryRequest): Flow<Document> {
//        return client.queryPaginated(query)
//            .mapNotNull { response -> response.items }
//            .flatMapConcat { items -> items.asFlow() }
//            .map(DynamoDbDocument::parse)
//    }

    suspend fun createTable(configure: CreateTableRequest.Builder.() -> Unit = { }) {
        val request: CreateTableRequest = CreateTableRequest {
            tableName = this@DynamoDbDocumentStore.tableName
            keySchema = listOf(
                KeySchemaElement {
                    attributeName = PARTITION_KEY
                    keyType = KeyType.Hash
                },
                KeySchemaElement {
                    attributeName = SORT_KEY
                    keyType = KeyType.Range
                }
            )
            attributeDefinitions = listOf(
                AttributeDefinition {
                    attributeName = PARTITION_KEY
                    attributeType = ScalarAttributeType.S
                },
                AttributeDefinition {
                    attributeName = SORT_KEY
                    attributeType = ScalarAttributeType.S
                }
            )
            billingMode = BillingMode.PayPerRequest

            configure()
        }

        client.createTable(request);
    }
}
