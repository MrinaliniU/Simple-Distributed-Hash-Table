package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    static final String KEY_FIELD = "key";
    static final String VALUE_FIELD = "value";
    String queryString =null;
    String portStr = null;
    String myPort = null;
    public Uri mUri=null;


    /* delete function */

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        int numberOfFilesDeleted = 0;
        if (selection.equals("@")){
            for(String selectionIterator : getContext().fileList()) {
                if(getContext().deleteFile(selectionIterator)){
                    numberOfFilesDeleted++;
                }
            }
                return numberOfFilesDeleted;
        }else if(selection.equals("*")){

            for(String selectionIterator : getContext().fileList()) {
                getContext().deleteFile(selectionIterator);
            }

        }
        else if((!selection.equals("@") && !selection.equals("*")))
        {
            try {
                if(DistributedHashTableHelper.compareNodes(selection, portStr)) {
                    //getContext().deleteFile(selection);
                    getContext().getContentResolver().delete(uri,selection,null);
                }else{
                    sendDelete(MessageState.DEL_NEXT.toString(),selection,myPort);
                    Thread.sleep(1000);
                    getContext().getContentResolver().delete(uri,selection,null);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return numberOfFilesDeleted;
    }

    /* Query or Node arrange tasks */
    private void sendMessage(String... message){
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }


    private void sendQuery(String... message){
        new ClientQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    private void sendDelete(String... message){
        new ClientQuery().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, message);
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = (String)values.get("key");
        String value = (String)values.get("value");
        try {
            if(DistributedHashTableHelper.compareNodes(key, portStr)) {
                FileOutputStream outputStream = getContext().openFileOutput(key, getContext().MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();
            }
            else {
                String nodeSuccessor = String.valueOf((Integer.parseInt(DistributedHashTableHelper.getSuccessorNode()) * 2));
                sendMessage(MessageState.NEXT_NODE.toString(), nodeSuccessor, key, value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return uri;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    @Override
    public boolean onCreate() {
        mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(getContext().TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        DistributedHashTableHelper.setSuccessorNode(portStr);
        DistributedHashTableHelper.setPredecessorNode(portStr);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            e.printStackTrace();
        }
        sendMessage(MessageState.NEW_NODE.toString(),myPort, portStr);
        return true;
    }

    /* query based on selection type */
    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        FileInputStream inputStream;
        MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
        BufferedReader bufferedReader;
        InputStreamReader inputStreamReader;
        if(selection.equals("@")){
            for (String selectionIterator : getContext().fileList()) {
                try {
                    inputStream = getContext().openFileInput(selectionIterator);
                    inputStreamReader = new InputStreamReader(inputStream);
                    bufferedReader = new BufferedReader(inputStreamReader);
                    String key = selectionIterator;
                    String value = bufferedReader.readLine();
                    matrixCursor.newRow().add(key).add(value);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                catch (IOException e) {
                    e.printStackTrace();
                }

            }
            return matrixCursor;

        }else if(selection.equals("*")){
            String queryAll= DistributedHashTableHelper.generateQueryString("",getContext());
            sendQuery(MessageState.QUERY.toString(), portStr, queryAll);
            try{
                Thread.sleep(1000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
            for(String queryString : queryString.split(":"))
            {
                String[] keyValuePairs = queryString.split("-");
                String key = keyValuePairs[0];
                String value = keyValuePairs[1];
                matrixCursor.newRow().add(key).add(value);
            }
            return matrixCursor;

        }else{
            try {
                if(DistributedHashTableHelper.compareNodes(selection, portStr)) {
                    String fetchedQuery= DistributedHashTableHelper.generateQueryString(selection,getContext());
                    String[] messageString=fetchedQuery.split(":");
                    String key=messageString[0];
                    String value=messageString[1];
                    matrixCursor.newRow().add(key).add(value);
                    return matrixCursor;
                }
                else {
                    sendQuery(MessageState.Q_NEXT.toString(),selection,myPort);
                    Thread.sleep(1000);
                    String[] messageString= queryString.split(":");
                    String key=messageString[0];
                    String value=messageString[1];
                    matrixCursor.newRow().add(key).add(value);
                    return matrixCursor;

                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /* Server task handles all types of Client Tasks either query or Node etc */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            Socket client = null;
            BufferedReader in;
            do {
                try {
                    client = serverSocket.accept();
                    in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    PrintWriter out = new PrintWriter(client.getOutputStream());
                    String messageReceived = in.readLine();
                    out.write("Got the msg at server " + messageReceived);
                    out.flush();
                    out.close();
                    if (messageReceived.contains(MessageState.NEW_NODE.toString())) {
                        String port = messageReceived.split(";")[1];
                        DistributedHashTableHelper.updateTree(port);
                        TreeMap<String,String> simpleDhtChord = DistributedHashTableHelper.setFinalNodeJoin();
                        for (String chordNodes : simpleDhtChord.keySet()) {
                            String[] messageString = simpleDhtChord.get(chordNodes).split(";");
                            String portNumber = String.valueOf((Integer.parseInt(messageString[0]) * 2));
                            sendMessage(MessageState.REARRANGE_CHORD.toString(), portNumber, messageString[1], messageString[2]);
                        }
                    }
                    if (messageReceived.contains(MessageState.NEXT_NODE.toString())) {
                        String[] messageString = messageReceived.split(";");
                        String key = messageString[1];
                        String value = messageString[2];
                        ContentValues values = new ContentValues();
                        values.put(KEY_FIELD, key);
                        values.put(VALUE_FIELD, value);
                        insert(mUri, values);
                    }
                    if (messageReceived.contains(MessageState.Q_LAST.toString())) {
                        queryString = messageReceived.split(";")[1];
                    }
                    if (messageReceived.contains(MessageState.QUERY.toString())) {
                        try {
                            String[] queryMessage = messageReceived.split(";");
                            String currentNode = queryMessage[1];
                            String queryString;
                            if (queryMessage.length > 2) {
                                queryString = queryMessage[2];
                            } else
                                queryString = "";
                            if (currentNode.equals(portStr)) {
                                SimpleDhtProvider.this.queryString = queryString;
                            } else {
                                String queryForward = queryString + DistributedHashTableHelper.generateQueryString("",getContext());
                                sendQuery(MessageState.QUERY.toString(), currentNode, queryForward);
                            }
                        } catch (ArrayIndexOutOfBoundsException e) {
                            e.printStackTrace();
                        }
                    }
                    if (messageReceived.contains(MessageState.Q_NEXT.toString())) {
                        String[] queryMessageString = messageReceived.split(";");
                        String selection = queryMessageString[1];
                        String currentNode = queryMessageString[2];
                        if (DistributedHashTableHelper.compareNodes(selection, portStr)) {
                            String finalFetchedQuery = DistributedHashTableHelper.generateQueryString(selection, getContext());
                            sendQuery(MessageState.Q_LAST.toString(), currentNode, finalFetchedQuery);
                        } else {
                            sendQuery(MessageState.Q_NEXT.toString(), selection, currentNode);
                        }
                    }
                    if (messageReceived.contains(MessageState.REARRANGE_CHORD.toString())) {
                        String[] keyValuePair = messageReceived.split(";");
                        DistributedHashTableHelper.setSuccessorNode(keyValuePair[1]);
                        DistributedHashTableHelper.setPredecessorNode(keyValuePair[2]);
                    }
                    in.close();
                    client.close();
                }catch (IOException e) {
                    e.printStackTrace();
                }catch (NoSuchAlgorithmException e){
                    e.printStackTrace();
                }

            } while (!client.isInputShutdown());
            return null;
        }

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }
}
