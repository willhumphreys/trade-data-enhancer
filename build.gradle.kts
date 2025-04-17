plugins {
    id("java")
    id("application")
    id("com.github.johnrengelman.shadow") version "8.1.1"
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
    implementation("org.ta4j:ta4j-core:0.15")


    implementation(platform("software.amazon.awssdk:bom:2.31.23"))

    // AWS SDK modules without versions
    implementation("software.amazon.awssdk:s3")
    implementation("software.amazon.awssdk:ssm")
    implementation("software.amazon.awssdk:sso")         // For SSO support
    implementation("software.amazon.awssdk:ssooidc")     // For SSO authentication

    // For LZO decompression
    implementation("org.anarres.lzo:lzo-core:1.0.6")


}


tasks.test {
    useJUnitPlatform()
}

application {
    mainClass.set("uk.co.threebugs.Main")
}

tasks.register<JavaExec>("runWithDefaults") {
    group = "application"
    description = "Runs the application with default parameters for quick testing"

    mainClass.set("uk.co.threebugs.Main")
    args = listOf("-w", "14", "-f", "btcusd_1-min_data.csv")
    classpath = sourceSets["main"].runtimeClasspath
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(23))
    }
}

tasks.shadowJar {
    archiveBaseName.set("app")
    archiveClassifier.set("")
    archiveVersion.set("")

    mergeServiceFiles()
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

tasks.jar {
    enabled = false
}

tasks.assemble {
    dependsOn(tasks.shadowJar)
}