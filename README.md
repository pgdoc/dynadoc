# Dynadoc

![Maven Central Version](https://img.shields.io/maven-central/v/org.dynadoc/dynadoc)

Dynadoc is a Kotlin library for using DynamoDB as a JSON document store. It manages the mapping between Kotlin objects and JSON documents.

## Concepts

### The `JsonEntity<T>` type

In the application code, documents are represented using the `IJsonEntity<T>` type:

```kotlin
data class JsonEntity<out T>(
    /** The unique identifier of the document. **/
    val id: DocumentKey,

    /** The body of the document deserialized into an object, or null if the document does not exist. **/
    val entity: T,

    /** The current version of the document. **/
    val version: Long
)
```

The `Entity` property can be null if the document does not exist. This can be the case for a document that hasn't been created yet, or for a document that has been deleted.

## License

Copyright 2023 Flavien Charlon

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
