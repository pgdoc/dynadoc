package org.dynadoc.core

import kotlinx.coroutines.flow.*
import kotlinx.coroutines.future.await
import org.dynadoc.core.AttributeMapper.PARTITION_KEY
import org.dynadoc.core.AttributeMapper.SORT_KEY
import org.dynadoc.core.AttributeMapper.VERSION
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.dynamodb.model.*

class DynamoDbDocumentStore(
    private val client: DynamoDbAsyncClient,
    private val tableName: String
) : DocumentStore {

    override suspend fun updateDocuments(updatedDocuments: Iterable<Document>, checkedDocuments: Iterable<Document>) {
        fun conditionExpression(version: Long) =
            if (version == 0L) {
                "attribute_not_exists($PARTITION_KEY)"
            } else {
                "$VERSION = :version"
            }

        fun expressionAttributeValues(version: Long) =
            if (version == 0L) {
                null
            } else {
                mapOf(":version" to AttributeValue.fromN(version.toString()))
            }

        val updates: Sequence<TransactWriteItem> = updatedDocuments.asSequence().map { document ->
                TransactWriteItem.builder()
                    .put(Put.builder()
                        .tableName(tableName)
                        .item(AttributeMapper.fromDocument(document))
                        .conditionExpression(conditionExpression(document.version))
                        .expressionAttributeValues(expressionAttributeValues(document.version))
                        .build())
                    .build()
            }

        val checks: Sequence<TransactWriteItem> = checkedDocuments.asSequence().map { document ->
                TransactWriteItem.builder()
                    .conditionCheck(ConditionCheck.builder()
                        .tableName(tableName)
                        .key(AttributeMapper.getKeyAttributes(document.id))
                        .conditionExpression(conditionExpression(document.version))
                        .expressionAttributeValues(expressionAttributeValues(document.version))
                        .build())
                    .build()
            }

        val elements = (updates + checks).toList();

        try {
            val write: TransactWriteItemsRequest = TransactWriteItemsRequest.builder()
                .transactItems(elements)
                .build()

            client.transactWriteItems(write).await()

        } catch (exception: TransactionCanceledException) {
            val checkFailureIndex: Int = exception.cancellationReasons()
                ?. indexOfFirst { it.code() == "ConditionalCheckFailed" }
                ?: -1

            if (checkFailureIndex >= 0) {
                val failure: TransactWriteItem = elements[checkFailureIndex]
                val attributes: Map<String, AttributeValue> = failure.put()?.item()
                    ?: failure.conditionCheck()?.key()!!

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

        val request: BatchGetItemRequest = BatchGetItemRequest.builder()
            .requestItems(mapOf(
                tableName to KeysAndAttributes.builder()
                    .keys(idList.distinct().map(AttributeMapper::getKeyAttributes))
                    .consistentRead(true)
                    .build()
                ))
            .build()

        val result: Flow<List<Document>> = flow {
            val responses = client.batchGetItem(request).await().responses()

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

    fun query(queryRequest: QueryRequest.Builder.() -> Unit): Flow<Document> {
        val query: QueryRequest = QueryRequest.builder()
            .tableName(tableName)
            .apply(queryRequest)
            .build()

        return flow {
            var currentPageQuery = query

            while (true) {
                val response = client.query(currentPageQuery).await()

                for (item in response.items()) {
                    emit(AttributeMapper.toDocument(item))
                }

                if (!response.hasLastEvaluatedKey()) {
                    break
                }

                currentPageQuery = query.toBuilder()
                    .exclusiveStartKey(response.lastEvaluatedKey())
                    .build()
            }
        }
    }

    fun scan(scanRequest: ScanRequest.Builder.() -> Unit): Flow<Document> {
        val query: ScanRequest = ScanRequest.builder()
            .tableName(tableName)
            .apply(scanRequest)
            .build()

        return flow {
            var currentPageQuery = query

            while (true) {
                val response = client.scan(currentPageQuery).await()

                for (item in response.items()) {
                    emit(AttributeMapper.toDocument(item))
                }

                if (!response.hasLastEvaluatedKey()) {
                    break
                }

                currentPageQuery = query.toBuilder()
                    .exclusiveStartKey(response.lastEvaluatedKey())
                    .build()
            }
        }
    }

    suspend fun createTable(configure: CreateTableRequest.Builder.() -> Unit = { }) {
        val request: CreateTableRequest = CreateTableRequest.builder()
            .tableName(tableName)
            .keySchema(listOf(
                KeySchemaElement.builder()
                    .attributeName(PARTITION_KEY)
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName(SORT_KEY)
                    .keyType(KeyType.RANGE)
                    .build()
            ))
            .attributeDefinitions(listOf(
                AttributeDefinition.builder()
                    .attributeName(PARTITION_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build(),
                AttributeDefinition.builder()
                    .attributeName(SORT_KEY)
                    .attributeType(ScalarAttributeType.S)
                    .build()
            ))
            .billingMode(BillingMode.PAY_PER_REQUEST)
            .apply(configure)
            .build()

        client.createTable(request).await()
    }
}