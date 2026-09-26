plugins {
    java
    application
}

group = "com.quince"
version = "0.1.0"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

sourceSets {
    create("integrationTest") {
        java.srcDir("src/integrationTest/java")
        compileClasspath += sourceSets["main"].output + sourceSets["test"].output
        runtimeClasspath += sourceSets["main"].output + sourceSets["test"].output
    }
}

configurations["integrationTestImplementation"].extendsFrom(configurations["testImplementation"])
configurations["integrationTestRuntimeOnly"].extendsFrom(configurations["testRuntimeOnly"])

dependencies {
    implementation(platform("software.amazon.awssdk:bom:2.54.17"))
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.19.1"))

    implementation("org.apache.kafka:kafka-clients:4.3.1")
    implementation("io.lettuce:lettuce-core:6.7.1.RELEASE")
    implementation("software.amazon.awssdk:dynamodb")
    implementation("software.amazon.awssdk:apache-client")
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")
    runtimeOnly("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(platform("org.junit:junit-bom:5.10.2"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    "integrationTestImplementation"(platform("org.testcontainers:testcontainers-bom:1.21.4"))
    "integrationTestImplementation"("org.testcontainers:junit-jupiter")
    "integrationTestImplementation"("org.testcontainers:kafka")
}

application {
    mainClass.set("com.quince.cartrecovery.Main")
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}

val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests against Docker (Testcontainers)."
    group = "verification"
    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath
    useJUnitPlatform()
    jvmArgs("-Djdk.tracePinnedThreads=full")
    shouldRunAfter(tasks.test)
    testLogging {
        events("passed", "failed", "skipped")
        showStandardStreams = false
    }
}

tasks.check {
    dependsOn(integrationTest)
}
