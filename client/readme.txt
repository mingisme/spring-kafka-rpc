mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081 -Dspring-boot.run.jvmArguments="-Dpartition=1"
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8082 -Dspring-boot.run.jvmArguments="-Dpartition=2"
