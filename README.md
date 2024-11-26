# Real-Time Streaming Data Pipeline

This project implements a real-time streaming data pipeline using Kafka and Docker. It processes user login data, performs basic analytics, and stores the processed data in a separate Kafka topic.

## Prerequisites

- Docker and Docker Compose
- Python 3.7+
- pip (Python package installer)

## Setup

1. Clone this repository:
   ```
   git clone git@github.com:aaron-lau/fetch-kafka.git
   cd fetch-kafka
   ```

2. Set up a Python virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
   ```

3. Install the required Python packages:
   ```
   pip install -r requirements.txt
   ```

## Running the Pipeline

1. Start the Kafka clusters and data generator:
   ```
   docker-compose up -d
   ```

2. Wait for a minute to ensure all services are up and running.

3. Run the Python script to process the data:
   ```
   python kafka_consumer_processor.py
   ```

4. The script will start processing messages from the 'user-login' topic and produce processed messages to the 'processed-logins' topic.

5. To stop the script, press Ctrl+C.

6. To shut down the Docker containers:
   ```
   docker-compose down
   ```

## Project Structure

- `docker-compose.yml`: Defines the Docker services (Kafka brokers, Zookeeper, data generator).
- `kafka_consumer_processor.py`: The main Python script that consumes, processes, and produces Kafka messages.
- `requirements.txt`: Lists the Python dependencies.

## Kafka Topics

- `user-login`: Input topic (on broker at localhost:29092)
- `processed-logins`: Output topic (on broker at localhost:29093)

## Customization

- To modify the processing logic, edit the `process_message` function in `kafka_consumer_processor.py`.
- To change Kafka configurations, edit the `docker-compose.yml` file.

## Troubleshooting

- If you encounter connection issues, ensure that the Kafka brokers are running and accessible.
- Check Docker logs for any service startup issues:
  ```
  docker-compose logs
  ```

## Additional Notes

- The data generator produces sample user login data continuously.
- The processor script provides basic insights every 100 processed messages.
- Ensure you have sufficient system resources to run multiple Docker containers and the Python script simultaneously.

## Design Choices and Architecture

### Data Flow

1. Data Generation: Sample user login data is continuously generated and published to the 'user-login' Kafka topic.
2. Data Consumption: Our Python script consumes messages from the 'user-login' topic.
3. Processing: Each message is processed to extract relevant information and perform basic analytics.
4. Data Production: Processed data is then published to the 'processed-logins' Kafka topic.
5. Analytics: Basic insights are generated and displayed in real-time as data is processed.

### Design Choices

1. Separation of Concerns: We use separate Kafka brokers for input and output data, allowing for independent scaling and management of raw and processed data streams.
2. Docker Containerization: All components are containerized, ensuring consistency across different environments and easy deployment.
3. Python for Processing: We chose Python for its rich ecosystem of data processing libraries and ease of use.
4. Real-time Processing: The system processes data as it arrives, providing up-to-date insights.

### Efficiency, Scalability, and Fault Tolerance

1. Efficiency:
   - Kafka's log-based architecture allows for high-throughput, low-latency data transfer.
   - In-memory processing in Python enables quick data manipulation and analysis.

2. Scalability:
   - Kafka's distributed nature allows for horizontal scaling by adding more brokers or partitions.
   - The consumer can be scaled by running multiple instances and leveraging Kafka's consumer group functionality.
   - Separate brokers for input and output allow independent scaling based on traffic patterns.

3. Fault Tolerance:
   - Kafka provides data replication (configurable) to prevent data loss in case of broker failures.
   - The consumer uses auto-commit to ensure that successfully processed messages are not reprocessed in case of failures.
   - Error handling in the Python script prevents crashes due to malformed messages or processing errors.
   - Docker's restart policies ensure that services can recover from temporary failures.

4. Monitoring and Maintenance:
   - Basic monitoring is implemented through console logging of processing statistics.
   - Kafka and Zookeeper logs are available through Docker for debugging and monitoring.

## Production, Deployment and Scaling

### 1. How would you deploy this application in production?

To deploy this application in a production environment, we would follow these steps:

a. Infrastructure Setup:
   - Use a cloud provider (e.g., AWS, GCP, Azure) for hosting.
   - Set up Kafka clusters using managed services like AWS MSK, Confluent Cloud, or self-managed on EC2/Kubernetes.
   - Use container orchestration (e.g., Kubernetes) for managing the Python processing application.

b. CI/CD Pipeline:
   - Implement a CI/CD pipeline using tools like Jenkins, GitLab CI, or GitHub Actions.
   - Automate testing, building Docker images, and deploying to staging/production environments.

c. Configuration Management:
   - Use tools like AWS Parameter Store or HashiCorp Vault for managing secrets and configurations.
   - Implement different configurations for development, staging, and production environments.

d. Monitoring and Logging:
   - Set up centralized logging (e.g., Datadog, ELK stack, Splunk).
   - Implement monitoring and alerting (e.g., Prometheus, Grafana, PagerDuty).

e. Security:
   - Implement network security groups and VPCs.
   - Use SSL/TLS for Kafka connections.
   - Implement authentication and authorization for Kafka.

f. Disaster Recovery:
   - Set up regular backups of Kafka data.
   - Implement a multi-region deployment for high availability.

### 2. What other components would you want to add to make this production ready?

To make this application production-ready, we would consider adding:

a. Data Validation and Schema Management:
   - Implement Kafka Schema Registry to ensure data consistency.

b. Stream Processing Framework:
   - Integrate a mature stream processing framework like Apache Flink or Kafka Streams for more complex processing.

c. Data Storage:
   - Add a data sink (e.g., Elasticsearch, Cassandra) for long-term storage of processed data.

d. API Layer:
   - Develop a RESTful API for querying processed data and insights.

e. Web Dashboard:
   - Create a web-based dashboard for real-time visualization of insights.

f. Alerting System:
   - Implement alerts for system health and business metrics.

g. Data Governance:
   - Add data lineage tracking and audit logging.

h. Performance Optimization:
   - Implement caching layers (e.g., Redis) for frequently accessed data.

i. Testing Infrastructure:
   - Set up comprehensive unit, integration, and end-to-end testing.

### 3. How can this application scale with a growing dataset?

To scale this application with a growing dataset:

a. Kafka Scaling:
   - Increase the number of partitions in Kafka topics to allow more parallel processing.
   - Add more Kafka brokers to handle increased load.

b. Consumer Scaling:
   - Run multiple instances of the Python consumer application.
   - Leverage Kafka's consumer groups for parallel processing.

c. Processing Optimization:
   - Optimize the Python code for performance.
   - Consider using PyPy or Cython for CPU-intensive tasks.
   - Consider moving off of Python if you want to truely take advantage of lower level programming.

d. Distributed Processing:
   - Implement a distributed processing framework (e.g., Apache Spark) for complex computations.

e. Database Scaling:
   - If using a database for storing processed data, ensure it can scale horizontally (e.g., use sharding).
   - Implement a data tiering strategy, moving older data to cheaper storage solutions (S3).
   - Use efficient data formats (e.g., Avro, Parquet) for data storage and transfer.

f. Load Balancing:
   - Use load balancers to distribute incoming data across multiple processing units.

g. Auto-Scaling:
   - Implement auto-scaling for the processing units based on input data volume or processing lag.

By implementing these strategies, the application can efficiently handle growing datasets while maintaining performance and reliability.