plugins {
    id("java")
    application
}

group = "uk.co.threebugs"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")

    // JUnit for testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.24.2")
    testCompileOnly("org.projectlombok:lombok:1.18.30")
    testAnnotationProcessor("org.projectlombok:lombok:1.18.30")

    // Apache Commons CLI
    implementation("commons-cli:commons-cli:1.5.0")
    implementation("ch.qos.logback:logback-classic:1.5.15")
    implementation("org.jetbrains:annotations:24.0.1")
}

tasks.test {
    useJUnitPlatform()
}

// Set up the application plugin
application {
    // Define the main class of the application
    mainClass = "uk.co.threebugs.Main"
}

// Add a custom task to run the application with default arguments
tasks.register<JavaExec>("runWithDefaults") {
    group = "application"
    description = "Runs the application with default parameters for quick testing"

    // Specify the main class
    mainClass.set("uk.co.threebugs.Main")

    // Pass default arguments
    args = listOf("-w", "14", "-f", "btcusd_1-min_data.csv")

    // Use the classpath defined in the project
    classpath = sourceSets["main"].runtimeClasspath
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(23))
    }
}