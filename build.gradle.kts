plugins {
    id("java")
}

group = "uk.co.threebugs"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    // Lombok
    compileOnly("org.projectlombok:lombok:1.18.30") // Add Lombok as a compile-only dependency
    annotationProcessor("org.projectlombok:lombok:1.18.30") // Enable annotation processing for Lombok

    // JUnit for testing
    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testCompileOnly("org.projectlombok:lombok:1.18.30") // For tests
    testAnnotationProcessor("org.projectlombok:lombok:1.18.30") // Enable annotation processing for tests
}

tasks.test {
    useJUnitPlatform()
}
