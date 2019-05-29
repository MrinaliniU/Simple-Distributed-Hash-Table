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

public class ClientTask extends AsyncTask<String, Void, Void> {

    private void sendMessage(String[] msgs, MessageState MSG) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(msgs[1]));
        if (socket.isConnected()) {
            PrintStream ps = new PrintStream(socket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            ps.println(MSG.toString() + ";" + msgs[2] + ";" + msgs[3]);
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
            if(msgToSend.equals(MessageState.NEW_NODE.toString())){
                String portLocal = msgs[2];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt("11108"));
                if (socket.isConnected()) {
                    PrintStream ps = new PrintStream(socket.getOutputStream());
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    ps.println(msgToSend+";"+ portLocal);
                    ps.flush();
                    Log.i(TAG, "Server sent " + in.readLine());
                    in.close();
                    ps.close();
                    socket.close();
                }
            }else if(msgToSend.equals(MessageState.NEXT_NODE.toString())){
                sendMessage(msgs, MessageState.NEXT_NODE);
            }else if(msgToSend.equals(MessageState.REARRANGE_CHORD.toString())){
                sendMessage(msgs, MessageState.REARRANGE_CHORD);
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
