Example for read & write into Impala with security
==================

Package for saagie : mvn package and get the package in target.

Usage in local :

 - mvn package
 - java -jar target/example-java-read-and-write-from-impala-with-security-1.0-SNAPSHOT-jar-with-dependencies.jar jdbc:hive2://impalahost:21050/;auth=PLAIN user password
Usage in Saagie :

 - mvn package (in local, to generate jar file)
 - create new Java Job
 - upload the jar (target/example-java-read-and-write-from-impala-with-security-1.0-SNAPSHOT-jar-with-dependencies.jar)
 - copy URL from Impala connection details panel and add it in first argument in the command line
 - copy user environment variable and add it in second argument in the command line
 - copy password environment variable and add it in third argument in the command line
 - choose java 8
 - create and launch.
