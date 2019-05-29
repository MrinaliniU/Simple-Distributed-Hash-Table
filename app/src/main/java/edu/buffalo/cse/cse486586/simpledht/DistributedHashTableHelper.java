package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class DistributedHashTableHelper {

    /* The Chord Ring Structure is similar to a LinkedList. Similar to the logic used in previous assignment the messages
     * can be passed between nodes via ClientTask to the ServerTask. Hence, below a Map DataStructure is used to pass both
     * chord nodes and their state.
     * A HashMap or LinkedHashMap does not maintain the order of storage. To be able to form a Ring Structure ordering needs to maintained during storage.
     * Hence TreeMap from SortedMap family is used. It implements a RED/Black tree Structure. A comparator is also defined for the TreeMap.
     * Reference : https://stackoverflow.com/questions/1936462/java-linkedhashmap-get-first-or-last-entry
     */

    private static String successorNode ="";
    private static String predecessorNode ="";
    private static TreeMap<String,String> chordNodes = new TreeMap<String,String>();
    private static TreeMap<String,String> simpleDhtChord = new TreeMap<String,String>();

    public static boolean compareNodes(String portNumber, String portStr)
    {
        try {
            boolean isFirstNode = successorNode.equals(portStr) && portStr.equals(predecessorNode);
            boolean isNextNode = genHash(portNumber).compareTo(genHash(predecessorNode)) > 0;
            boolean isQueryNode = isNextNode && genHash(portNumber).compareTo(genHash(portStr)) <= 0;
            boolean isFinalQueryNode = genHash(predecessorNode).compareTo(genHash(portStr)) > 0 && (((genHash(portNumber).compareTo(genHash(portStr)) < 0) || isNextNode));
            boolean isSendMessage;
            if(isFinalQueryNode | isQueryNode | isFirstNode)
                isSendMessage=true;
            else
                isSendMessage=false;
            return isSendMessage;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return false;
    }

    /* Based on if it is a successor or a predecessor the chord update String is generated */

    private static  String updateCord(String selectionString, boolean isSuccessor)
    {
        String cordString = "";
        if(isSuccessor){
            Set keys = chordNodes.keySet();
            Iterator iterator = keys.iterator();
            while (iterator.hasNext()) {
                String hashKey = (String)iterator.next();
                try {
                    if(hashKey.equals(chordNodes.lastKey()))
                    {
                        Map.Entry<String,String> entry = chordNodes.firstEntry();
                        cordString = entry.getValue().split(";")[0];
                        break;
                    }
                    else
                    {
                        if(hashKey.equals(selectionString)){
                            String hashKeyNext = (String) iterator.next();
                            cordString = chordNodes.get(hashKeyNext).split(";")[0];
                            break;
                        }
                    }
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
            return cordString;
        }else{
            for(String nodes: chordNodes.keySet())
            {
                if(selectionString.equals(chordNodes.firstKey()))
                {
                    Map.Entry<String,String> entry= chordNodes.lastEntry();
                    cordString = entry.getValue().split(";")[0];
                    break;
                }
                else
                {
                    if(selectionString.equals(nodes)){
                        break;
                    }
                    cordString = chordNodes.get(nodes).split(";")[0];
                }
            }
            return cordString;
        }

    }

    /* Method to update Chord structure with Succesor and Predecessor Node */

    public static void updateTree(String port) throws NoSuchAlgorithmException{
        chordNodes.put(genHash(port), port + ";" + successorNode + ";" + predecessorNode);
    }

    /* Once all Node joins are successfully completed final cord structure is created */

    public static TreeMap<String, String> setFinalNodeJoin(){
        for (String nodes : chordNodes.keySet()) {
            String portNumber = chordNodes.get(nodes).split(";")[0];
            try {
                simpleDhtChord.put(genHash(portNumber), portNumber + ";" + updateCord(nodes,true) + ";" + updateCord(nodes,false));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }

        return simpleDhtChord;
    }

    /* Depending on if it is query all or query current note message string is formed */

    public static String generateQueryString(String selection, Context context){
        FileInputStream inputStream;
        BufferedReader bufferedReader;
        InputStreamReader inputStreamReader;
        String queryString = "";
        String value = "";

        if(selection.equals("")){

            for (String selectionList : context.fileList()) {
                try{
                    inputStream = context.openFileInput(selectionList);
                    inputStreamReader = new InputStreamReader(inputStream);
                    bufferedReader = new BufferedReader(inputStreamReader);
                    value = bufferedReader.readLine();
                    queryString = queryString + selectionList + "-" + value + ":";
                }catch(FileNotFoundException e){
                    e.printStackTrace();

                }catch (IOException e){
                    e.printStackTrace();
                }
            }
            return queryString;
        }else{

            try{
                inputStream = context.openFileInput(selection);
                inputStreamReader = new InputStreamReader(inputStream);
                bufferedReader = new BufferedReader(inputStreamReader);
                value = bufferedReader.readLine();

            }catch (FileNotFoundException e){
                e.printStackTrace();
            }catch(IOException e){
                e.printStackTrace();
            }
            queryString = selection + ":" + value;
            return queryString;
        }
    }

    /* setters */

    public static void setSuccessorNode(String successorNode){
        DistributedHashTableHelper.successorNode = successorNode;
    }

    public static void setPredecessorNode(String predecessorNode){
        DistributedHashTableHelper.predecessorNode = predecessorNode;
    }

    /* getters */

    public static String getPredecessorNode(){
        return DistributedHashTableHelper.predecessorNode;
    }

    public static String getSuccessorNode(){
        return DistributedHashTableHelper.successorNode;
    }

    /* Hash Value Generator */

    private static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

}
