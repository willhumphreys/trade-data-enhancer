# Build stage
FROM eclipse-temurin:23-jdk AS build

WORKDIR /app

# Copy Gradle files first for better layer caching
COPY build.gradle.kts settings.gradle.kts ./
COPY gradle gradle
COPY gradlew ./

# Make the wrapper executable
RUN chmod +x ./gradlew

# Download dependencies
RUN ./gradlew dependencies --no-daemon

# Copy source code
COPY src src

# Build the application with Shadow JAR
RUN ./gradlew shadowJar --no-daemon

# Runtime stage
FROM eclipse-temurin:23-jre

# Install lzop for LZO decompression
RUN apt-get update && \
    apt-get install -y lzop && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Create directories for data
RUN mkdir -p data/minute output

# Copy the Shadow JAR from the build stage
COPY --from=build /app/build/libs/app.jar app.jar

# Run the application
ENTRYPOINT ["java", "-jar", "app.jar"]