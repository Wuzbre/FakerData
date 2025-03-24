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

# Create necessary directories
create_directories() {
    print_info "Creating necessary directories..."
    
    # Create logs directory
    mkdir -p logs
    
    # Ensure all script directories exist
    mkdir -p src/main/java
    mkdir -p src/main/scala
    mkdir -p src/main/resources
    mkdir -p datax
    mkdir -p doris
    mkdir -p hive
    mkdir -p scripts
    
    print_success "Directories created successfully!"
}

# Initialize Maven project if pom.xml doesn't exist
initialize_maven_project() {
    if [ ! -f "pom.xml" ]; then
        print_info "Initializing Maven project..."
        
        # Check if Maven is installed
        if ! command -v mvn &> /dev/null; then
            print_error "Maven is not installed. Please install Maven or use './mvnw' after running this script."
            return 1
        fi
        
        # Create a simple pom.xml
        cat > pom.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.sales</groupId>
    <artifactId>sales-data-warehouse</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
        <scala.version>2.12.15</scala.version>
        <scala.binary.version>2.12</scala.binary.version>
        <spark.version>3.3.1</spark.version>
        <hadoop.version>3.3.2</hadoop.version>
        <typesafe.config.version>1.4.2</typesafe.config.version>
        <log4j.version>2.17.2</log4j.version>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
        <!-- Scala -->
        <dependency>
            <groupId>org.scala-lang</groupId>
            <artifactId>scala-library</artifactId>
            <version>${scala.version}</version>
        </dependency>

        <!-- Spark Core -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Spark SQL -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_${scala.binary.version}</artifactId>
            <version>${spark.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Hadoop -->
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-client</artifactId>
            <version>${hadoop.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- Configuration -->
        <dependency>
            <groupId>com.typesafe</groupId>
            <artifactId>config</artifactId>
            <version>${typesafe.config.version}</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-api</artifactId>
            <version>${log4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-core</artifactId>
            <version>${log4j.version}</version>
        </dependency>
        <dependency>
            <groupId>org.apache.logging.log4j</groupId>
            <artifactId>log4j-slf4j-impl</artifactId>
            <version>${log4j.version}</version>
        </dependency>

        <!-- MySQL Connector -->
        <dependency>
            <groupId>mysql</groupId>
            <artifactId>mysql-connector-java</artifactId>
            <version>8.0.30</version>
        </dependency>

        <!-- Command line parser -->
        <dependency>
            <groupId>com.github.scopt</groupId>
            <artifactId>scopt_${scala.binary.version}</artifactId>
            <version>4.1.0</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <!-- Scala Compiler -->
            <plugin>
                <groupId>net.alchim31.maven</groupId>
                <artifactId>scala-maven-plugin</artifactId>
                <version>4.6.3</version>
                <executions>
                    <execution>
                        <goals>
                            <goal>compile</goal>
                            <goal>testCompile</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <scalaVersion>${scala.version}</scalaVersion>
                </configuration>
            </plugin>

            <!-- Assembly Plugin for creating a fat JAR -->
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.4.2</version>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                    <archive>
                        <manifest>
                            <mainClass>com.sales.SalesDataETL</mainClass>
                        </manifest>
                    </archive>
                </configuration>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
EOF
        
        print_success "Maven project initialized successfully!"
    else
        print_info "Maven project already initialized (pom.xml exists)"
    fi
}

# Create Maven wrapper if not exists
create_maven_wrapper() {
    if [ ! -f "mvnw" ]; then
        print_info "Creating Maven wrapper..."
        
        # Check if Maven is installed for creating wrapper
        if command -v mvn &> /dev/null; then
            mvn -N io.takari:maven:wrapper
            print_success "Maven wrapper created successfully!"
        else
            print_warning "Maven is not installed. Skipping Maven wrapper creation."
        fi
    else
        print_info "Maven wrapper already exists"
    fi
}

# Create basic configuration files if they don't exist
create_config_files() {
    # application.conf
    if [ ! -f "src/main/resources/application.conf" ]; then
        print_info "Creating application.conf..."
        
        mkdir -p src/main/resources
        cat > src/main/resources/application.conf << 'EOF'
spark {
  master = "local[*]"
  app.name = "Sales Data Warehouse ETL"
  sql.warehouse.dir = "/user/hive/warehouse"
}

mysql {
  url = "jdbc:mysql://localhost:3306/sales_db"
  user = "root"
  password = "password"
  driver = "com.mysql.cj.jdbc.Driver"
}

hive {
  metastore.uris = "thrift://localhost:9083"
  ods.database = "purchase_sales_ods"
  dwd.database = "purchase_sales_dwd"
  dws.database = "purchase_sales_dws"
  dq.database = "purchase_sales_dq"
}

doris {
  url = "jdbc:mysql://localhost:9030/purchase_sales"
  user = "root"
  password = "password"
  database = "purchase_sales"
}

tables {
  # Source tables from MySQL
  mysql.tables = [
    "users",
    "employees",
    "products",
    "sales_orders",
    "sales_order_items",
    "purchase_orders",
    "purchase_order_items",
    "suppliers"
  ]
  
  # Tables to sync to Doris
  doris.sync.tables = [
    "dwd_dim_user",
    "dwd_dim_employee",
    "dwd_dim_product",
    "dwd_dim_supplier",
    "dwd_fact_sales_order",
    "dwd_fact_sales_order_item",
    "dwd_fact_purchase_order",
    "dwd_fact_purchase_order_item",
    "dws_sales_day_agg",
    "dws_sales_month_agg",
    "dws_product_sales",
    "dws_customer_behavior",
    "dws_supplier_purchase"
  ]
}

etl {
  # ETL date, default is yesterday
  date = ""
  
  # Data quality thresholds
  dq {
    ods.null.ratio.threshold = 0.05
    ods.volume.threshold = 0.7
    dwd.conversion.ratio.threshold = 0.95
  }
  
  # Data lifecycle management
  lifecycle {
    ods.retention.days = 7
    dwd.retention.days = 30
    dws.retention.days = 90
  }
  
  # Processing settings
  processing {
    parallelism = 4
    batch.size = 10000
  }
  
  # Data consistency check
  consistency {
    check.enabled = true
    threshold = 0.99
  }
}
EOF
        
        print_success "application.conf created successfully!"
    else
        print_info "application.conf already exists"
    fi
    
    # log4j2.xml
    if [ ! -f "src/main/resources/log4j2.xml" ]; then
        print_info "Creating log4j2.xml..."
        
        mkdir -p src/main/resources
        cat > src/main/resources/log4j2.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
        <RollingFile name="RollingFile" fileName="logs/etl.log" filePattern="logs/etl-%d{yyyy-MM-dd}-%i.log">
            <PatternLayout pattern="%d{yyyy-MM-dd HH:mm:ss} [%t] %-5level %logger{36} - %msg%n"/>
            <Policies>
                <TimeBasedTriggeringPolicy />
                <SizeBasedTriggeringPolicy size="10 MB"/>
            </Policies>
            <DefaultRolloverStrategy max="10"/>
        </RollingFile>
    </Appenders>
    <Loggers>
        <Root level="info">
            <AppenderRef ref="Console"/>
            <AppenderRef ref="RollingFile"/>
        </Root>
        <Logger name="org.apache.spark" level="warn" additivity="false">
            <AppenderRef ref="Console"/>
            <AppenderRef ref="RollingFile"/>
        </Logger>
    </Loggers>
</Configuration>
EOF
        
        print_success "log4j2.xml created successfully!"
    else
        print_info "log4j2.xml already exists"
    fi
}

# Make scripts executable
make_scripts_executable() {
    print_info "Making scripts executable..."
    
    chmod +x build.sh run-etl.sh setup.sh
    
    # Make all scripts in scripts directory executable
    if [ -d "scripts" ]; then
        chmod +x scripts/*.sh 2>/dev/null || true
    fi
    
    print_success "Scripts are now executable!"
}

# Main execution
main() {
    print_info "Setting up Sales Data Warehouse project..."
    
    create_directories
    initialize_maven_project
    create_maven_wrapper
    create_config_files
    make_scripts_executable
    
    print_success "Setup completed successfully!"
    print_info "You can now build the project with: ./build.sh"
    print_info "And run the ETL process with: ./run-etl.sh"
}

# Run the main function
main 