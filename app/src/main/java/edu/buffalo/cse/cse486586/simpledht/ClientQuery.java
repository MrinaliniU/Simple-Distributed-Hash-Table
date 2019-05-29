package edu.buffalo.cse.cse486586.simpledht;

import android.os.AsyncTask;
import android.util.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class ClientQuery extends AsyncTask<String, Void, Void> {

    private void sendMessage(String[] msgs,  MessageState MSG) throws IOException {
        String nextNode = String.valueOf((Integer.parseInt(DistributedHashTableHelper.getSuccessorNode()) * 2));
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(nextNode));
        if (socket.isConnected()) {
            PrintStream ps = new PrintStream(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ps.println(MSG.toString() + ";" + msgs[1] + ";" + msgs[2]);
            ps.flush();
            Log.i(TAG, "Server sent " + in.readLine());
            in.close();
            ps.close();
            socket.close();
        }
    }

    static final String TAG = ClientTask.class.getSimpleName();
    @Override
    protected Void doInBackground(String... msgs) {
        try {
            String msgToSend = msgs[0];
            if(msgToSend.equals(MessageState.Q_NEXT.toString())) {
                sendMessage(msgs,MessageState.Q_NEXT);
            }else if(msgToSend.equals(MessageState.QUERY.toString())) {
                sendMessage(msgs,MessageState.QUERY);
            }else if(msgToSend.equals(MessageState.Q_LAST.toString())) {
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
                if (socket.isConnected()) {
                    PrintStream ps = new PrintStream(socket.getOutputStream());
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    ps.println(MessageState.Q_LAST.toString() + ";" + msgs[2]);
                    ps.flush();
                    Log.i(TAG, "Server sent " + in.readLine());
                    in.close();
                    ps.close();
                    socket.close();
                }
            }
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            e.printStackTrace();
            Log.e(TAG, "ClientTask socket IOException");
        }

        return null;
    }
}
