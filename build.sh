#!/bin/bash

# Function to print colored output
print_info() {
    echo -e "\033[1;34m[INFO] $1\033[0m"
}

print_warning() {
    echo -e "\033[1;33m[WARNING] $1\033[0m"
}

print_error() {
    echo -e "\033[1;31m[ERROR] $1\033[0m"
}

print_success() {
    echo -e "\033[1;32m[SUCCESS] $1\033[0m"
}

# Check if Java is installed
if ! command -v java &> /dev/null; then
    print_error "Java is not installed. Please install JDK 8 or higher."
    exit 1
fi

# Get Java version
java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
print_info "Using Java version: $java_version"

# Build project
print_info "Building Sales Data Warehouse project..."

# Clean and package with Maven
if [ -f "./mvnw" ]; then
    print_info "Using Maven Wrapper..."
    ./mvnw clean package -DskipTests
    build_status=$?
else
    print_info "Using local Maven installation..."
    mvn clean package -DskipTests
    build_status=$?
fi

# Check build status
if [ $build_status -eq 0 ]; then
    # Check if the JAR file exists
    if [ -f "target/sales-data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar" ]; then
        print_success "Build successful! JAR file is located at: target/sales-data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar"
        print_info "To run the ETL process, use: ./run-etl.sh"
    else
        print_warning "Build completed but could not find the expected JAR file."
    fi
else
    print_error "Build failed with status code: $build_status"
    exit 1
fi 