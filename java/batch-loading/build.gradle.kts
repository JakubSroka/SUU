plugins {
    id("org.jetbrains.kotlin.jvm") version "1.3.72"
    application
}

group = "pl.edu.agh"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven(url = "http://download.oracle.com/maven")
    jcenter()
}

dependencies {
    val janusGraphVersion = "0.5.2"
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.janusgraph:janusgraph-core:$janusGraphVersion")
    implementation("org.janusgraph:janusgraph-es:$janusGraphVersion")
    implementation("org.janusgraph:janusgraph-hbase:$janusGraphVersion")
    implementation("org.janusgraph:janusgraph-cql:$janusGraphVersion")
    implementation("org.janusgraph:janusgraph-berkeleyje:$janusGraphVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.3.7")
    implementation("org.slf4j:slf4j-simple:1.7.30")
}

application {
    mainClassName = "loader.MainKt"
}