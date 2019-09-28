package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";
    static final String authority = "edu.buffalo.cse.cse486586.simpledynamo.provider";
    static final String scheme = "content";
    static final int SERVER_PORT = 10000;
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    static final String KEY_INITIALIZED = "initialized";
    static final String[] ports = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    String mMyPort = "", mNodeId = "";
    static final String delimiter = "~";
    Node mHead = null;
    HashMap<String, Node> mMap;
    Map<String, String> mKeyVal, mRecKeyMap;
    Map<String, List<String>> mQueries;
    MatrixCursor mCursor;
    int mQueryAllCount = 0, mRecoveryCount = 0;
    String mData = "";
    public List<String> mLivePortsList = new ArrayList<String>();
    boolean isRecovering = false;

	@Override
	public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        mMyPort = String.valueOf((Integer.parseInt(portStr) * 2));

        mMap = new HashMap<String, Node>();

        mKeyVal = new HashMap<String, String>();
        mQueries = new HashMap<String, List<String>>();

        Collections.addAll(mLivePortsList, ports);

        try {
            mNodeId = genHash(portStr);
            //Log.d(TAG, "Node id : " + mNodeId + " = " + mMyPort);
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Node NoSuchAlgorithmException");
        }

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Failed to create ServerSocket " + e.getMessage());
        }

        createCord();

        if (queryLocal(KEY_INITIALIZED).split("_")[0].equals("true")) {
            //Recovery Logic
            isRecovering = true;
            if (!mLivePortsList.contains(mMyPort))
                mLivePortsList.add(mMyPort);
            deleteAllLocal();
            recover();
        } else {
            insertLocal(KEY_INITIALIZED, "true", mMyPort);
        }
        return false;
	}

	public void recover() {
	    Log.d(TAG, "recover");

	    Node myNode = mMap.get(mMyPort);

	    mRecKeyMap = new HashMap<String, String>();
	    mRecoveryCount = 0;

        new SingleTask().executeOnExecutor(
                AsyncTask.THREAD_POOL_EXECUTOR, "recover", myNode.succ.port, myNode.port);

        new SingleTask().executeOnExecutor(
                AsyncTask.THREAD_POOL_EXECUTOR, "recover", myNode.succ.succ.port, myNode.port);

        new SingleTask().executeOnExecutor(
                AsyncTask.THREAD_POOL_EXECUTOR, "recover", myNode.pred.port, myNode.pred.port);

        new SingleTask().executeOnExecutor(
                AsyncTask.THREAD_POOL_EXECUTOR, "recover", myNode.pred.pred.port, myNode.pred.pred.port);
    }

    /* References:
       1. Socket programming:
       https://docs.oracle.com/javase/tutorial/networking/sockets/index.html
       2. Two-way communication via Socket:
       https://www.youtube.com/watch?v=-xKgxqG411c
       3. Keeping socket connection alive
       https://stackoverflow.com/questions/36135983/cant-send-multiple-messages-via-socket
     */
    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            try {
                while (true) {
                    Socket socket = serverSocket.accept();
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String message = in.readLine();
                    if (message != null) {
                        String[] result = message.split(delimiter);

                        if (result[0].equals("insert")) {
                            Log.d(TAG, "Message received by Server " + getClientName(mMyPort));
                            String key = result[1];
                            String value = result[2];
                            String coordPort = result[3];

                            publishProgress("insert_local", key, value, coordPort);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            String str = "ack" + "\n";
                            out.write(str);
                            out.flush();
                        } else if (result[0].equals("query")) {
                            String key = result[1];
                            Log.d(TAG, "Message received by Server " + getClientName(mMyPort) + ": " + key);

                            if (key.equals("*")) {
                                String data = queryAll("");

                                PrintWriter out = new PrintWriter(socket.getOutputStream());
                                String str = "ack_query" + delimiter + key + delimiter + data + "\n";
                                out.write(str);
                                out.flush();
                                continue;
                            }

                            String value = queryLocal(key);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            String str = "ack_query" + delimiter + key + delimiter + value + "\n";
                            out.write(str);
                            out.flush();
                        } else if (result[0].equals("delete")) {
                            String key = result[1];
                            Log.d(TAG, "Message received by Server " + getClientName(mMyPort) + ": " + key);

                            publishProgress("delete_local", key);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            String str = "ack" + "\n";
                            out.write(str);
                            out.flush();
                        } else if (result[0].equals("recover")) {
                            String port = result[1];
                            Log.d(TAG, "Message received by Server " + getClientName(mMyPort) + ": " + port);

                            String data = queryAll(port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            String str = "ack_recover" + delimiter + data + "\n";
                            out.write(str);
                            out.flush();
                        } else if (result[0].equals("replicas")) {
                            String port = result[1];
                            Log.d(TAG, "Message received by Server " + getClientName(mMyPort) + ": " + port);

                            String data = queryAll(port);
                            PrintWriter out = new PrintWriter(socket.getOutputStream());
                            String str = "ack_replicas" + delimiter + data + "\n";
                            out.write(str);
                            out.flush();
                        }
                    }
                }
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "ServerTask SocketTimeoutException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask Socket IOException");
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            if (values[0].equals("insert_local")) {
                insertLocal(values[1], values[2], values[3]);
            } else if (values[0].equals("delete_local")) {
                if (values[1].equals("*")) {
                    deleteAllLocal();
                } else {
                    deleteLocal(values[1]);
                }
            }
        }
    }

    public void createCord() {
        Node node0 = new Node(REMOTE_PORT4);
        Node node1 = new Node(REMOTE_PORT1);
        Node node2 = new Node(REMOTE_PORT0);
        Node node3 = new Node(REMOTE_PORT2);
        Node node4 = new Node(REMOTE_PORT3);

        node0.pred = node4;
        node0.succ = node1;
        node1.pred = node0;
        node1.succ = node2;
        node2.pred = node1;
        node2.succ = node3;
        node3.pred = node2;
        node3.succ = node4;
        node4.pred = node3;
        node4.succ = node0;

        mMap.put(REMOTE_PORT0, node2);
        mMap.put(REMOTE_PORT1, node1);
        mMap.put(REMOTE_PORT2, node3);
        mMap.put(REMOTE_PORT3, node4);
        mMap.put(REMOTE_PORT4, node0);

        mHead = node0;
    }

    public void insertLocal(String key, String value, String coordPort) {
        /*
            References:
            1. Content Provider Android Documentation
            https://developer.android.com/guide/topics/providers/content-provider-creating
            2. File storage Android Documentation
            https://developer.android.com/training/data-storage/files#java
            3. Reading and writing string from a file on Android
            https://stackoverflow.com/questions/14376807/how-to-read-write-string-from-a-file-in-android
            4. Overwriting a file in internal storage on Android
            https://stackoverflow.com/questions/36740254/how-to-overwrite-a-file-in-sdcard-in-android
         */

        int version;
        String val = queryLocal(key);
        if (!val.isEmpty()) {
            version = Integer.valueOf(val.split("_")[1]);
            version++;
        } else {
            version = 1;
        }

        String fileName = key;
        String fileContents = value + "_" + version + "_" + coordPort + "\n";
        insertFile(fileName, fileContents);
    }

    public void insertFile(String fileName, String fileContents) {
        FileOutputStream outputStream;
        try {
            File file = new File(getContext().getFilesDir(), fileName);
            if (file.exists()) {
                outputStream = new FileOutputStream(file, false);
            } else {
                outputStream = new FileOutputStream(file);
            }
            outputStream.write(fileContents.getBytes());
            outputStream.close();
            Log.d(TAG, "Key Inserted : " + fileName);
        } catch (Exception e) {
            Log.e(TAG, "Failed to write file.");
        }
    }

    public String queryLocal(String key) {
        String value = "";
        try {
            /*
                References:
                 1. Content Provider Android Documentation
                https://developer.android.com/guide/topics/providers/content-provider-creating
                2. Reading file contents from internal storage on Android
                https://stackoverflow.com/questions/14768191/how-do-i-read-the-file-content-from-the-internal-storage-android-app
             */
            if (getContext().openFileInput(key) == null) {
                Log.e(TAG, "NULL");
                return  "";
            }
            FileInputStream inputStream = getContext().openFileInput(key);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(inputStream));
            value = in.readLine();
            inputStream.close();
        } catch (Exception e) {
            //Log.e(TAG, "Failed to read file.");
        }
        return value;
    }

    public String queryAll(String port) {
        String data = "";
        File directory = new File(getContext().getFilesDir().toString());
        File[] files = directory.listFiles();

        if (files.length == 0)
            return data;

        for (int i = 0; i < files.length; i++) {
            if (files[i].getName().equals(KEY_INITIALIZED))
                continue;

            String value = queryLocal(files[i].getName());
            if (!port.isEmpty()) {
                String temp = value.split("_")[2];
                if (temp.equals(port)) {
                    data += files[i].getName() + ":" + value + ";";
                }
            } else {
                data += files[i].getName() + ":" + value + ";";
            }
        }

        if (!data.isEmpty() && data.charAt(data.length()-1) == ';') {
            data = data.substring(0, data.length()-1);
        }
        return data;
    }

    /*
        References:
        1. https://stackoverflow.com/questions/4943629/how-to-delete-a-whole-folder-and-content
     */
    public int deleteAllLocal() {
        File directory = new File(getContext().getFilesDir().toString());
        String[] filenames = directory.list();
        int size = filenames.length;
        for (String filename : filenames) {
            if (filename.equals(KEY_INITIALIZED))
                continue;
            new File(directory, filename).delete();
        }
        return size;
    }

    public void deleteLocal(String filename) {
        File directory = new File(getContext().getFilesDir().toString());
        new File(directory, filename).delete();
    }

    private class SingleTask extends AsyncTask<String, String, Void> {
        String curPort = "";
        String curKey = "";
        String curOp = "";

        @Override
        protected Void doInBackground(String... args) {
            try {
                String operation = args[0];
                String remotePort = args[1];
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(remotePort));
                PrintWriter out = new PrintWriter(socket.getOutputStream());

                if (operation.equals("recover")) {
                    socket.setSoTimeout(500);
                    curOp = "recover";
                }

                curPort = remotePort;
                String str = "";
                if (operation.equals("insert")) {
                    String key = args[2];
                    String value = args[3];
                    String coordPort = args[4];
                    str = operation + delimiter + key + delimiter + value + delimiter + coordPort + "\n";
                } else if (operation.equals("query")) {
                    String key = args[2];
                    curKey = key;
                    str = operation + delimiter + key + "\n";
                } else if (operation.equals("delete")) {
                    String key = args[2];
                    str = operation + delimiter + key + "\n";
                } else if (operation.equals("recover")) {
                    String port = args[2];
                    str = operation + delimiter + port + "\n";
                } else if (operation.equals("replicas")) {
                    String port = args[2];
                    str = operation + delimiter + port + "\n";
                }

                Log.d(TAG, "Message sent from Client " + getClientName(mMyPort) +
                        " to Server " + getClientName(remotePort) + " : " + str);
                out.write(str);
                out.flush();

                BufferedReader in = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                String reply = in.readLine();
                if (reply != null) {
                    String[] ack = reply.split(delimiter);
                    if (ack[0] != null) {
                        if (ack[0].equals("ack_query")) {
                            publishProgress(ack[1], ack.length == 3 ? ack[2] : "");
                        } else if (ack[0].equals("ack_recover")) {
                            publishProgress("recover", ack.length == 2 ? ack[1] : "");
                        } else if (ack[0].equals("ack_replicas")) {
                            publishProgress("replicas", ack.length == 2 ? ack[1] : "");
                        }

                        in.close();
                        out.close();
                        socket.close();
                    }
                }
                return null;
            } catch (SocketTimeoutException e) {
                Log.e(TAG, "SingleTask SocketTimeoutException");
                if (curOp.equals("recover")) {
                    handleRecovery("");
                }
                if (!curKey.isEmpty()) {

                }
            } catch (IOException e) {
                Log.e(TAG, "SingleTask Socket IOException");
                if (curOp.equals("recover")) {
                    handleRecovery("");
                }
                if (!curKey.isEmpty()) {
                    handleFailure(curKey, curPort);
                }
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            if (values[0].equals("recover")) {
                handleRecovery(values[1]);
            } else if (values[0].equals("replicas")) {
                handleQueryAll(values[1]);
            } else {
                if (!values[1].isEmpty()) {
                    //handleQuery(values[0], values[1]);
                }
            }
        }
    }

    private class MultiTask extends AsyncTask<String, String, Void> {
        String curPort = "";
        String curKey = "";

        @Override
        protected Void doInBackground(String... args) {
            String operation = args[0];
            String coordPort = args[1];
            Node coordinator = mMap.get(coordPort);
            String[] remotePorts = {coordinator.port, coordinator.succ.port, coordinator.succ.succ.port};

            for (String remotePort : remotePorts) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    //socket.setSoTimeout(2000);

                    curPort = remotePort;
                    String str = "";
                    if (operation.equals("insert")) {
                        String key = args[2];
                        String value = args[3];
                        str = operation + delimiter + key + delimiter + value + delimiter + coordPort + "\n";
                    } else if (operation.equals("query")) {
                        String key = args[2];
                        curKey = key;
                        str = operation + delimiter + key + "\n";
                    } else if (operation.equals("delete")) {
                        String key = args[2];
                        str = operation + delimiter + key + "\n";
                    } else if (operation.equals("recover")) {
                        String ports = args[2];
                        str = operation + delimiter + ports + "\n";
                    }

                    Log.d(TAG, "Message sent from Client " + getClientName(mMyPort) +
                            " to Server " + getClientName(remotePort) + " : " + str);
                    out.write(str);
                    out.flush();

                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String reply = in.readLine();
                    if (reply != null) {
                        Log.d(TAG, "ack: " + reply);
                        String[] ack = reply.split(delimiter);
                        if (ack[0] != null) {
                            if (ack[0].equals("ack_query")) {
                                publishProgress(ack[1], ack.length == 3 ? ack[2] : "");
                            }

                            in.close();
                            out.close();
                            socket.close();
                        }
                    } else {
                        Log.d(TAG, "ack: null");
                        throw new IOException();
                    }
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "MultiTask SocketTimeoutException");
                    if (!curKey.isEmpty()) {
                        //handleFailure(curKey, curPort);
                    }
                } catch (IOException e) {
                    Log.e(TAG, "MultiTask Socket IOException");
                    if (!curKey.isEmpty()) {
                        //handleFailure(curKey, curPort);
                        //handleQuery(curKey, "aux_0_11108");
                    }
                }
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            if (!values[1].isEmpty())
                handleQuery(values[0], values[1]);
        }
    }

    private class AllTask extends AsyncTask<String, String, Void> {
        String curPort = "";
        String curKey = "";

        @Override
        protected Void doInBackground(String... args) {
            String operation = args[0];
            for (String remotePort : ports) {
                try {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePort));
                    PrintWriter out = new PrintWriter(socket.getOutputStream());
                    //socket.setSoTimeout(SOCKET_TIMEOUT);

                    curPort = remotePort;
                    String str = "";
                    if (operation.equals("query")) {
                        String key = args[1];
                        curKey = key;
                        str = operation + delimiter + key + "\n";
                    } else if (operation.equals("delete")) {
                        String key = args[1];
                        str = operation + delimiter + key + "\n";
                    }

                    Log.d(TAG, "Message sent from Client " + getClientName(mMyPort) +
                            " to Server " + getClientName(remotePort) + " : " + str);
                    out.write(str);
                    out.flush();

                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(socket.getInputStream()));
                    String reply = in.readLine();
                    if (reply != null) {
                        Log.d(TAG, "ack: " + reply);
                        String[] ack = reply.split(delimiter);
                        if (ack[0] != null) {
                            if (ack[0].equals("ack_query")) {
                                publishProgress(ack[1], ack.length == 3 ? ack[2] : "");
                            }
                            in.close();
                            out.close();
                            socket.close();
                        }
                    } else {
                        Log.d(TAG, "ack: null");
                        throw new IOException();
                    }
                } catch (SocketTimeoutException e) {
                    Log.e(TAG, "AllTask SocketTimeoutException");
                    if (!curKey.isEmpty()) {
                        //handleFailure(curKey, curPort);
                    }
                } catch (IOException e) {
                    Log.e(TAG, "AllTask Socket IOException");
                    if (!curKey.isEmpty()) {
                        handleFailure(curKey, curPort);
                    }
                }
            }
            return null;
        }

        @Override
        protected void onProgressUpdate(String... values) {
            if (values[0].equals("*")) {
                handleQueryAll(values[1]);
            }
        }
    }

    public void handleQuery(String key, String value) {
        //Log.d(TAG, "handleQuery: " + key + ":" + value);
        if (!mQueries.containsKey(key))
            return;

        List<String> values = mQueries.get(key);
        values.add(value);
        mQueries.put(key, values);

        if (mQueries.get(key).size() == 2) {
            //Log.d(TAG, "handleQuery (Inside Statement): " + key);
            String recentValue = "";
            int max = 0;
            for (String str : values) {
                String[] arr = str.split("_");
                if (Integer.valueOf(arr[1]) > max) {
                    max = Integer.valueOf(arr[1]);
                    recentValue = arr[0];
                }
            }

            mQueries.remove(key);
            mKeyVal.put(key, recentValue);
            //Log.d(TAG, "handleQuery (Final): " + key + ":" + recentValue);
        }
    }

    public void handleQueryAll(String str) {
        mQueryAllCount++;
        if (!str.isEmpty()) {
            if (mData.isEmpty()) {
                mData = str;
            } else {
                mData = mData + ";" + str;
            }
        }

        if (mQueryAllCount == 5) {
            strToCursor(mData);
            mQueryAllCount = 0;
            mData = "";
        }
    }

    public void strToCursor(String data) {
        MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
        if (!data.isEmpty()) {
            String[] pairs = data.split(";");
            for (String pair : pairs) {
                String[] duo = pair.split(":");
                String[] arr = duo[1].split("_");
                cursor.addRow(new String[]{duo[0], arr[0]});
            }
        }
        mCursor = cursor;
    }

    public void handleFailure(String key, String failedPort) {
        mLivePortsList.remove(failedPort);
        if (key.equals("*")) {
            //handleQueryAll("");
            new SingleTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "replicas", mMap.get(failedPort).succ.port, failedPort);
        } else {
            //handleQuery(key, "aux_0_11108");
        }
    }

    public void handleRecovery(String data) {
        mRecoveryCount++;
        if (!data.isEmpty()) {
            String[] pairs = data.split(";");
            for (String pair : pairs) {
                String[] duo = pair.split(":");
                String[] arr = duo[1].split("_");

                String key = duo[0];
                String value = duo[1];
                int version = Integer.valueOf(arr[1]);

                /*String val = queryLocal(key);
                if (!val.isEmpty()) {
                    int curVersion = Integer.valueOf(val.split("_")[1]);
                    if (curVersion >= version)
                        continue;
                }*/
                //insertFile(key, value);

                if (mRecKeyMap.containsKey(key)) {
                    String val = mRecKeyMap.get(key);
                    int curVersion = Integer.valueOf(val.split("_")[1]);
                    if (curVersion >= version)
                        continue;
                }

                mRecKeyMap.put(key, value);
            }
        }

        if (mRecoveryCount == 4) {
            for (String key : mRecKeyMap.keySet()) {
                insertFile(key, mRecKeyMap.get(key));
            }
            isRecovering = false;
        }
    }

    public Node getCoordinator(String objectId) {
        Node ptr = mHead;
        while (true) {
            if (objectId.compareTo(ptr.pred.id) > 0 && objectId.compareTo(ptr.id) <= 0) {
                return ptr;
            } else if (objectId.compareTo(ptr.pred.id) > 0 && ptr.id.compareTo(ptr.pred.id) < 0) {
                return ptr;
            } else if (objectId.compareTo(ptr.id) < 0 && ptr.pred.id.compareTo(ptr.id) > 0) {
                return ptr;
            } else {
                ptr = ptr.succ;
            }
        }
    }

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.v("delete", selection);

        if (selection.equals("@")) {
            return deleteAllLocal();
        }

        if (selection.equals("*")) {
            new AllTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "delete", selection);
            return 0;
        }

        try {
            String objectId = genHash(selection);
            Node coordinator = getCoordinator(objectId);

            new MultiTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "delete", coordinator.port, selection);

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Delete NoSuchAlgorithmException");
        }
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
        Log.v("insert", values.toString());
        String key = values.getAsString(KEY_FIELD);
        String value = values.getAsString(VALUE_FIELD);

        try {
            String objectId = genHash(key);
            Node coordinator = getCoordinator(objectId);

            new MultiTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "insert", coordinator.port, key, value);

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Insert NoSuchAlgorithmException");
        }
        return uri;
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
        Log.v("query", selection);

        /*
            References:
            1. https://stackoverflow.com/questions/8646984/how-to-list-files-in-an-android-directory/8647397#8647397
         */
        if (selection.equals("@")) {
            while (isRecovering) {

            }

            File directory = new File(getContext().getFilesDir().toString());
            File[] files = directory.listFiles();
            MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
            for (File file : files) {
                if (file.getName().equals(KEY_INITIALIZED))
                    continue;
                String[] arr = queryLocal(file.getName()).split("_");
                cursor.addRow(new String[]{file.getName(), arr[0]});
            }
            return cursor;
        }

        if (selection.equals("*")) {
            new AllTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "query", selection);

            while (mCursor == null) {

            }

            MatrixCursor tempCursor = mCursor;
            mCursor = null;
            return tempCursor;
        }

        try {
            String objectId = genHash(selection);
            Node coordinator = getCoordinator(objectId);

            mQueries.put(selection, new ArrayList<String>());

            new MultiTask().executeOnExecutor(
                    AsyncTask.THREAD_POOL_EXECUTOR, "query", coordinator.port, selection);

            while (true) {
                if (mKeyVal.containsKey(selection)) {
                    MatrixCursor cursor = new MatrixCursor(new String[]{KEY_FIELD, VALUE_FIELD});
                    cursor.addRow(new String[]{selection, mKeyVal.get(selection)});
                    mKeyVal.remove(selection);
                    return cursor;
                }
            }

        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Query NoSuchAlgorithmException");
        }
        return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String getClientName(String portNumber) {
        switch (Integer.valueOf(portNumber)) {
            case 11108:
                return "1";
            case 11112:
                return "2";
            case 11116:
                return "3";
            case 11120:
                return "4";
            case 11124:
                return "5";
            default: return "";
        }
    }

    class Node {
        String id;
        String port;
        Node pred;
        Node succ;

        Node(String port) {
            this.port = port;

            try {
                this.id = genHash(String.valueOf(Integer.valueOf(port)/2));
            } catch (NoSuchAlgorithmException e) {
                Log.e(TAG, "Node NoSuchAlgorithmException");
            }
        }
    }
}




















