## Simple Java Sync Server For Automerge

### Start the server
```
mvn clean install
mvn spring-boot:run
```

### Test with a client

* Clone https://github.com/inkandswitch/tiny-essay-editor
* Update the websocket url in main.tsx to "ws://localhost:8080/merge"
