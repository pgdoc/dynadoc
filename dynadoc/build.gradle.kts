dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.8.1")
    api("software.amazon.awssdk:dynamodb-enhanced:[2.21.0, 3[")

    testImplementation(kotlin("test"))

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("io.mockk:mockk:1.13.11")
    testImplementation("org.testcontainers:testcontainers:1.19.8")
    testImplementation("org.testcontainers:junit-jupiter:1.19.8")
    testImplementation("org.skyscreamer:jsonassert:1.5.1")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name = "Dynadoc"
                description = "Dynadoc is a Kotlin library for using DynamoDB as a JSON document store."
                url = "https://github.com/pgdoc/dynadoc"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        name = "Flavien Charlon"
                        email = "flavien@charlon.org"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/pgdoc/dynadoc.git"
                    url = "https://github.com/pgdoc/dynadoc/tree/master"
                }
            }
        }
    }

    repositories {
        maven {
            url = uri(layout.buildDirectory.dir("../../publish/${project.name}-${project.version}"))
        }
    }
}

signing {
    sign(publishing.publications["maven"])
}
