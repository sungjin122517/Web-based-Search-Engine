# Requirements
- JDK 21 or higher
- Apache Maven 3.9.6 or higher

# How to Build
Run the following command to build the project:
```shell
mvn clean package
```
This will create an uber-jar in the `target` directory named `course_project-1.0-SNAPSHOT.jar`.

# How to Execute
Run the following command to generate spider_result.txt:
```shell
java -jar target/course_project-1.0-SNAPSHOT.jar spider_result
```

Run the following command to crawl pages:
```shell
java -jar target/course_project-1.0-SNAPSHOT.jar crawl
```
