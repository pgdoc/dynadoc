allprojects {
    group = "org.dynadoc"
    version = "1.0.1-SNAPSHOT"
}

plugins {
    // Apply the org.jetbrains.kotlin.jvm plugin to add support for Kotlin.
    kotlin("jvm") version "1.9.23"
    id("maven-publish")
    id("signing")
}

subprojects {
    apply(plugin = "org.jetbrains.kotlin.jvm")
    // Apply the java-library plugin for API and implementation separation.
    apply(plugin = "java-library")
    apply(plugin = "maven-publish")
    apply(plugin = "signing")

    kotlin {
        jvmToolchain(11)
    }

    repositories {
        mavenCentral()
    }

    tasks.named<Test>("test") {
        // Use JUnit Platform for unit tests.
        useJUnitPlatform()
    }

    configure<JavaPluginExtension> {
        withSourcesJar()
        withJavadocJar()
    }
}

repositories {
    mavenCentral()
}