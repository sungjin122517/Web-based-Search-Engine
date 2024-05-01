# Requirements
- JDK 21
- Apache Maven 3.9.6

# How to Build
Run the following command to build the project:
```shell
mvn clean package -DskipTests
```
This will create an uber-jar in the `target` directory named `course_project-1.0-SNAPSHOT.jar`.

# How to Execute
Run the following command to crawl pages:
```shell
java -jar target/course_project-1.0-SNAPSHOT.jar crawl
```
Then, run the following command to start server:
```shell
java -jar target/course_project-1.0-SNAPSHOT.jar server
```
To access the search engine, open a web browser and navigate to `http://localhost:8080`.
