package org.dynadoc.serialization

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer

class KotlinJsonSerializer(
    private val kotlinJson: Json
) : JsonSerializer {

    override fun serialize(entity: Any): String =
        kotlinJson.encodeToString(
            serializer(entity.javaClass),
            entity)

    override fun <T : Any> deserialize(json: String, clazz: Class<T>): T =
        kotlinJson.decodeFromString(
            serializer(clazz) as KSerializer<T>,
            json)
}
