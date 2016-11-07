make -j4
rmiregistry &
java -cp "bin:lib/protobuf.jar" Client
