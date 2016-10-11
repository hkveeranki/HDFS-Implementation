EXE:src/*.java Proto HDFS
	javac -cp ".:lib/protobuf.jar" -sourcepath src src/*.java -d bin/
Proto:hdfs.proto
	protoc --java_out=src hdfs.proto
HDFS:src/HDFS/hdfs.java
	javac -cp ".:lib/protobuf.jar" src/HDFS/hdfs.java -d bin/
clean:
	rm -rf bin/*.class bin/HDFS
