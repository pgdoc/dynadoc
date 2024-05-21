package org.dynadoc.serialization

object TestSerializer : JsonSerializer {
    private val regex = Regex("\\{\"string\":\"(?<value>[^\"]*)\"}")

    fun jsonFor(value: String) = "{\"string\":\"$value\"}"

    override fun serialize(entity: Any): String {
        if (entity is String) {
            return jsonFor(entity)
        } else {
            error("Type not supported.")
        }
    }

    override fun <T : Any> deserialize(json: String, clazz: Class<T>): T {
        require(clazz == String::class.java)

        val match: MatchResult? = regex.matchEntire(json)
        require(match != null)

        val result = match.groups["value"]
        require(result != null)

        return result.value as T
    }
}
