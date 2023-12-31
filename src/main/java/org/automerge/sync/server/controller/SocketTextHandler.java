package org.automerge.sync.server.controller;

import com.upokecenter.cbor.CBORObject;
import org.automerge.Document;
import org.automerge.SyncState;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.BinaryMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.BinaryWebSocketHandler;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class SocketTextHandler extends BinaryWebSocketHandler {

    // document-id -> Document
    private final Map<String, Document> documents = new HashMap<>();

    // document-id + sender-id -> Sync State
    private final Map<String, SyncState> syncStates = new ConcurrentHashMap<>();

    // document -> sender-ids
    private final Map<String, Set<String>> clientSubscriptions = new ConcurrentHashMap<>();

    // sender-id -> -> session object
    private final Map<String, WebSocketSession> clientSessions = new ConcurrentHashMap<>();

    @Override
    protected void handleBinaryMessage(WebSocketSession session, BinaryMessage message) throws Exception {

        ByteBuffer payload = message.getPayload();
        CBORObject cborObject = CBORObject.DecodeFromBytes(payload.array());
        CBORObject objectType = cborObject.get("type");

        if (!objectType.isNull()) {
            if ("join".equals(objectType.AsString())) {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                CBORObject respObj = CBORObject.NewMap();
                String senderId = cborObject.get("senderId").AsString();
                respObj.Add("type", "peer");
                respObj.Add("senderId", "storage-server-sync-automerge-org");
                respObj.Add("selectedProtocolVersion", "1");
                respObj.Add("targetId", senderId);
                respObj.WriteTo(output);
                output.flush();
                session.sendMessage(new BinaryMessage(output.toByteArray()));

                if (clientSessions.containsKey(senderId)) {
                    try {
                        clientSessions.get(senderId).close();
                    } catch (Exception e) {
                        // Ignore
                    }
                }

                // Store the session for future syncs
                clientSessions.put(senderId, session);

            } else if ("sync".equals(objectType.AsString()) || "request".equals(objectType.AsString())) {

                String targetId = cborObject.get("targetId").AsString();
                byte[] data = cborObject.get("data").GetByteString();
                String documentId = cborObject.get("documentId").AsString();
                String senderId = cborObject.get("senderId").AsString();

                if (!documents.containsKey(documentId)) {
                    documents.put(documentId, new Document());
                }

                if (!syncStates.containsKey(documentId + ":" + senderId)) {
                    syncStates.put(documentId + ":" + senderId, new SyncState());
                }

                Document document = documents.get(documentId);
                SyncState syncState = syncStates.get(documentId + ":" + senderId);

                document.receiveSyncMessage(syncState, data);
                Optional<byte[]> generatedSyncMessageOp = document.generateSyncMessage(syncState);

                // Sync with peer

                CBORObject respObj = CBORObject.NewMap();
                respObj.Add("type", "sync");
                respObj.Add("senderId", "storage-server-sync-automerge-org");

                if (generatedSyncMessageOp.isPresent()) {
                    // If there are sync changes, send it
                    respObj.Add("data", generatedSyncMessageOp.get());
                } else {
                    // If there is no sync changes, Send same data object the client has sent
                    respObj.Add("data", data);
                }

                respObj.Add("targetId", senderId);
                respObj.Add("documentId", documentId);
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                respObj.WriteTo(output);
                output.flush();
                session.sendMessage(new BinaryMessage(output.toByteArray()));

                if (!clientSubscriptions.containsKey(documentId)) {
                    clientSubscriptions.put(documentId, new HashSet<>());
                }

                Set<String> senderIds = clientSubscriptions.get(documentId);
                senderIds.add(senderId);

                // Sync across all the other subscribers
                for (String syncSenderId : senderIds.stream().toList()) {

                    if (syncSenderId.equals(senderId)) {
                        continue;
                    }

                    if (!syncStates.containsKey(documentId + ":" + syncSenderId)) {
                        syncStates.put(documentId + ":" + syncSenderId, new SyncState());
                    }
                    syncState = syncStates.get(documentId + ":" + syncSenderId);

                    generatedSyncMessageOp = document.generateSyncMessage(syncState);

                    WebSocketSession syncSenderSession = clientSessions.get(syncSenderId);
                    if (generatedSyncMessageOp.isPresent() && syncSenderSession.isOpen()) {
                        respObj = CBORObject.NewMap();
                        respObj.Add("type", "sync");
                        respObj.Add("senderId", "storage-server-sync-automerge-org");
                        respObj.Add("data", generatedSyncMessageOp.get());
                        respObj.Add("targetId", syncSenderId);
                        respObj.Add("documentId", documentId);
                        output = new ByteArrayOutputStream();
                        respObj.WriteTo(output);
                        output.flush();
                        syncSenderSession.sendMessage(new BinaryMessage(output.toByteArray()));
                    }
                }
            }
        }
    }
}
