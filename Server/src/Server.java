import javax.xml.soap.SAAJMetaFactory;
import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.DatagramChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.sql.rowset.CachedRowSet;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.PGSimpleDataSource.*;
import com.sun.rowset.CachedRowSetImpl;
import sun.reflect.generics.tree.Tree;

/**
 * Created by Денис on 21.05.2017.
 */
public class Server {
    private static final int SSH_PORT = 22;
    private static boolean[] needsRefreshing=new boolean[11];
    private static ConcurrentLinkedQueue<Integer> portQueue=new ConcurrentLinkedQueue<Integer>();
    private static final String HOSTNAME = "52.174.16.235";
    private static final String USERNAME = "kjkszpj361";
    private static final String PASSWORD = "B9zbYEl*dj}6";

    public static void main(String args[]) throws Exception {
        Class c = Class.forName("Human");
        Object obj = c.newInstance();
        System.out.println(obj.getClass());
        c.getName();
        for (int i = 8880; i <= 8890; i++) {
            monitorPort(i);
        }
    }

    public static byte[] serializeTree(TreeSet Col) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(Col);
            out.flush();
            byte[] serializedCollection = bos.toByteArray();
            bos.close();
            return serializedCollection;
        } catch (IOException ex) {
            // ignore close exception
        }finally {
            try{
                bos.close();}catch (IOException e){}
        }
        return null;
    }

    public static Object deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
            System.out.println(o.getClass());
            return o;
        } catch(IOException e){e.printStackTrace();}catch (ClassNotFoundException as){as.getCause();as.getMessage();}
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException ex) {
                // ignore close exception
            }

        }
        return null;
    }
    public static void monitorPort(int port) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("Starting to monitor port " + port);
                    needsRefreshing[port - 8880] = false;
                    PGSimpleDataSource source1 = new PGSimpleDataSource();
                    source1.setDatabaseName("postgres");
                    source1.setPortNumber(5432);
                    source1.setServerName("localhost");
                    source1.setUser("kebab");
                    source1.setPassword("123456");
                    DatagramChannel serverChannel = DatagramChannel.open();
                    final SocketAddress clientAddress;
                    serverChannel.bind(new InetSocketAddress(port));
                    byte[] receiveData = new byte[8192];
                    ByteBuffer receiveBuffer = ByteBuffer.wrap(receiveData);
                    receiveBuffer.clear();
                    ByteBuffer sendBuffer = ByteBuffer.wrap(("Connected to port " + port).getBytes());
                    sendBuffer.clear();
                    clientAddress = serverChannel.receive(receiveBuffer);
                    System.out.println("Someone connected to port " + port + " from " + getHostname(clientAddress).toString());
                    serverChannel.send(sendBuffer, clientAddress);
                    Connection connection1 = null;
                    try {
                        connection1 = source1.getConnection();
                    } catch (SQLException e) {
                        System.out.println("BAGA");
                    }
                        try{
                        TreeSet kkk = pullCollection(connection1, receiveData);
                        System.out.println("Starting to serve port " + port);
                        System.out.println();
                        byte[] k = serializeTree(kkk);
                        serverChannel.send(ByteBuffer.wrap(k), clientAddress);
                        servePort(serverChannel, clientAddress, port);
                    }
                        catch (Exception e) {
                        System.out.println("Это не коллекция");
                        TreeSet kkk = new TreeSet();
                        byte[] k = serializeTree(kkk);
                        serverChannel.send(ByteBuffer.wrap(k), clientAddress);
                        servePort(serverChannel, clientAddress, port);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static void servePort(DatagramChannel serverChannel1, SocketAddress clientAddress1, int port) {
        Thread serveThread = new Thread(new Runnable() {
            @Override
            public void run() {
                PGSimpleDataSource source = new PGSimpleDataSource();
                source.setDatabaseName("postgres");
                source.setPortNumber(5432);
                source.setServerName("localhost");
                source.setUser("kebab");
                source.setPassword("123456");
                Connection connection = null;
                //LabCollection returnCollection = new LabCollection();
                SocketAddress clientAddress;
                try {
                    connection = source.getConnection();
                    System.out.println("Connected to database");
                } catch (SQLException e) {
                    System.out.println("Cannot connect to database");
                }
                DatagramChannel serverChannel = serverChannel1;
                TimeoutThread receiveTimeout = new TimeoutThread(Thread.currentThread(), 120000);
                receiveTimeout.interrupt();
                try {
                    receiveTimeout.start();
                    System.out.println("Timer for client on port " + port + " started");
                    while (receiveTimeout.getState() != Thread.State.WAITING) {
                        byte[] receiveData = new byte[8192];
                        ByteBuffer receiveBuffer = ByteBuffer.wrap(receiveData);
                        receiveBuffer.clear();
                        byte[] sendData = new byte[8192];
                        ByteBuffer sendBuffer = ByteBuffer.wrap(sendData);
                        String sentence;
                        sendBuffer.clear();
                        while (true) {
                            receiveFromAddress(serverChannel, clientAddress1, receiveBuffer);
                            receiveTimeout.sleepTime = 120000;
                            receiveTimeout.interrupt();
                            receiveBuffer.flip();
                            byte[] bytes = new byte[receiveBuffer.remaining()];
                            receiveBuffer.get(bytes);
                            sentence = new String(bytes);
                            receiveBuffer.clear();
                            if (!sentence.contains("test")) break;
                        }
                        System.out.println("Client send command " + sentence + " on port " + port);
                        clientAddress = receiveFromAddress(serverChannel, clientAddress1, receiveBuffer);
                        portQueue.add(port);
                        while (true) {
                            if (portQueue.peek() == port) break;
                        }
                        receiveBuffer.flip();
                        byte[] humanBytes = new byte[receiveBuffer.remaining()];
                        receiveBuffer.get(humanBytes);
                        System.out.println(humanBytes.getClass());
                        System.out.println(deserialize(humanBytes).getClass());
                        System.out.println("Received Human or Collection from client on port" + port);
                        if (needsRefreshing[port - 8880]) {
                            System.out.println("But Client's data needs refreshing, so command will be ignored");
                            serverChannel.send(ByteBuffer.wrap("true".getBytes()), clientAddress);
                            if (sentence.contains("update")) {
                                serverChannel.receive(receiveBuffer);
                                receiveBuffer.clear();
                                serverChannel.receive(receiveBuffer);
                                receiveBuffer.clear();
                            }
                            sentence = "collection";
                        } else {
                            serverChannel.send(ByteBuffer.wrap("false".getBytes()), clientAddress);
                        }
                        switch (sentence) {
                            case "disconnect": {
                                System.out.println("Disconnecting port" + port);
                                throw (new ClosedByInterruptException());
                            }
                            case "collection": {
                                System.out.println("Refreshing data on port " + port);
                                needsRefreshing[port - 8880] = false;
                            }
                            break;
                            case "remove": {
                                try {
                                    Class aclass = deserialize(humanBytes).getClass();
                                    Class[] paramTypes = new Class[] {};
                                    Object[] args = new Object[] {};
                                    Method method1 = aclass.getMethod("getName", paramTypes);
                                    String name = (String) method1.invoke(deserialize(humanBytes), args);
                                    Method method2 = aclass.getMethod("getAge", paramTypes);
                                    int age = (int) method2.invoke(deserialize(humanBytes), args);
                                    Method method3 = aclass.getMethod("getLocation", paramTypes);
                                    String location = (String) method3.invoke(deserialize(humanBytes), args);
                                    PreparedStatement st = connection.prepareStatement("delete from Humans where (name = ?) and (age = ?) and (location = ?);");
                                    st.setString(1, name);
                                    st.setInt(2, age);
                                    st.setString(3, location);
                                    System.out.println("Removing object from collection... " + port);
                                    st.execute();
                                } catch (SQLException e) {
                                } catch (NoSuchMethodException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                }
                                break;
                            }
                            case "remove_lower": {
                                try {
                                    connection.setAutoCommit(false);
                                    Class aclass = deserialize(humanBytes).getClass();
                                    Class[] paramTypes = new Class[] {};
                                    Object[] args = new Object[] {};
                                    Method method1 = aclass.getMethod("getName", paramTypes);
                                    Method method2 = aclass.getMethod("getAge", paramTypes);
                                    Method method3 = aclass.getMethod("getLocation", paramTypes);
                                    PreparedStatement st1 = connection.prepareStatement("select * from Humans;");
                                    System.out.println("Removing objects lower than received object... " + port);
                                    ResultSet rs = st1.executeQuery();
                                    CachedRowSet cs = new CachedRowSetImpl();
                                    cs.populate(rs);
                                    TreeSet col = new TreeSet<>();
                                    while (cs.next()) {
                                        Constructor constructor = aclass.getConstructor(String.class, int.class, String.class);
                                        Object newObject = deserialize(humanBytes).getClass().newInstance();
                                        newObject = constructor.newInstance(cs.getString("name"),cs.getInt("age"),cs.getString("location"));
                                        col.add(newObject);
                                    }
                                    Iterator iterator = col.iterator();
                                    while (iterator.hasNext()) {
                                        Object A = iterator.next();
                                        Class[] paramTypes2 = new Class[] {Object.class};
                                        Object[] args2 = new Object[] {deserialize(humanBytes)};
                                        Method method4 = aclass.getMethod("compareTo", paramTypes2);
                                        int integer = (int) method4.invoke(A, args);
                                        String name1 = (String) method1.invoke(A, args);
                                        int age1 = (int) method2.invoke(A, args);
                                        String location1 = (String) method3.invoke(A, args);
                                        if (integer < 0) {
                                            iterator.remove();
                                            PreparedStatement st = connection.prepareStatement("delete from Humans where (name = ?) and (age = ?) and (location = ?);");
                                            st.setString(1, name1);
                                            st.setInt(2, age1);
                                            st.setString(3,location1);
                                            st.execute();
                                        }
                                    }
                                    connection.commit();
                                    connection.setAutoCommit(true);
                                } catch (SQLException e) {
                                } catch (NoSuchMethodException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                } catch (InstantiationException e) {
                                    e.printStackTrace();
                                }
                                break;
                            }
                            case "update": {
                                try {
                                    Class aclass = deserialize(humanBytes).getClass();
                                    Class[] paramTypes = new Class[] {};
                                    Object[] args = new Object[] {};
                                    Method method1 = aclass.getMethod("getName", paramTypes);
                                    String name = (String) method1.invoke(deserialize(humanBytes), args);
                                    Method method2 = aclass.getMethod("getAge", paramTypes);
                                    int age = (int) method2.invoke(deserialize(humanBytes), args);
                                    Method method3 = aclass.getMethod("getLocation", paramTypes);
                                    String location = (String) method3.invoke(deserialize(humanBytes), args);
                                    byte[] updateBytes = new byte[8192];
                                    ByteBuffer updateBuffer = ByteBuffer.wrap(updateBytes);
                                    updateBuffer.clear();
                                    receiveFromAddress(serverChannel, clientAddress1, updateBuffer);
                                    receiveTimeout.sleepTime = 120000;
                                    receiveTimeout.interrupt();
                                    updateBuffer.flip();
                                    int attributeNumber = updateBuffer.getInt();
                                    System.out.println("Received attribute number from client " + attributeNumber + " " + port);
                                    updateBuffer.clear();
                                    clientAddress = receiveFromAddress(serverChannel, clientAddress1, updateBuffer);
                                    receiveTimeout.sleepTime = 120000;
                                    receiveTimeout.interrupt();
                                    updateBuffer.flip();
                                    byte[] serNewValue = new byte[updateBuffer.remaining()];
                                    updateBuffer.get(serNewValue);
                                    String newValue = new String(serNewValue);
                                    System.out.println("Received attribute value from client: " + newValue + " " + port);
                                    PreparedStatement st;
                                    switch (attributeNumber) {
                                        case 1: {
                                            st = connection.prepareStatement("update Humans set name=? where (name=?) and (age=?) and (location=?);");
                                        }
                                        break;
                                        case 2: {
                                            st = connection.prepareStatement("update Humans set age=? where (name=?) and (age=?) and (location=?);");
                                        }
                                        break;
                                        case 3: {
                                            st = connection.prepareStatement("update Humans set location=? where (name=?) and (age=?) and (location=?);");
                                        }
                                        break;
                                        default: {
                                            st = connection.prepareStatement("update Humans set ?=? where (name=?) and (age=?) and (location=?);");
                                        }
                                    }
                                    if (attributeNumber == 2) {
                                        st.setInt(1, Integer.parseInt(newValue));
                                    } else {
                                        st.setString(1, newValue);
                                    }
                                    st.setString(2, name);
                                    st.setInt(3, age);
                                    st.setString(4, location);
                                    st.executeUpdate();
                                } catch (SQLException e) {
                                    System.out.println("Something went wrong " + port);
                                } catch (NoSuchMethodException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                }
                                break;
                            }
                            case "add": {
                                try {
                                    Class aclass = deserialize(humanBytes).getClass();
                                    Class[] paramTypes = new Class[] {};
                                    Object[] args = new Object[] {};
                                    Method method1 = aclass.getMethod("getName", paramTypes);
                                    String name = (String) method1.invoke(deserialize(humanBytes), args);
                                    Method method2 = aclass.getMethod("getAge", paramTypes);
                                    int age = (int) method2.invoke(deserialize(humanBytes), args);
                                    Method method3 = aclass.getMethod("getLocation", paramTypes);
                                    String location = (String) method3.invoke(deserialize(humanBytes), args);
                                    PreparedStatement st = connection.prepareStatement("insert into Humans (name,age,location) values(?,?,?)");
                                    st.setString(1, name);
                                    st.setInt(2, age);
                                    st.setString(3, location);
                                    System.out.println("Adding new object to database... " + port);
                                    st.executeUpdate();
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                } catch (NoSuchMethodException e) {
                                    e.printStackTrace();
                                } catch (IllegalAccessException e) {
                                    e.printStackTrace();
                                } catch (InvocationTargetException e) {
                                    e.printStackTrace();
                                }
                                break;
                            }
                            case "import": {
                                Class aclass = deserialize(humanBytes).getClass();
                                Class[] paramTypes = new Class[] {};
                                Object[] args = new Object[] {};
                                Method method1 = aclass.getMethod("getUselessData", paramTypes);
                                TreeSet col = (TreeSet) method1.invoke(deserialize(humanBytes),args);
                                Iterator iterator = col.iterator();
                                PreparedStatement st = null;
                                try {
                                    connection.setAutoCommit(false);
                                    st = connection.prepareStatement("insert into Humans (name,age,location) values(?,?,?)");
                                    while (iterator.hasNext()) {
                                        Object A = col.first();
                                        Object newObject = A.getClass().newInstance();
                                        Method method11 = aclass.getMethod("getName", paramTypes);
                                        String name = (String) method11.invoke(newObject, args);
                                        Method method21 = aclass.getMethod("getAge", paramTypes);
                                        int age = (int) method21.invoke(newObject, args);
                                        Method method31 = aclass.getMethod("getLocation", paramTypes);
                                        String location = (String) method31.invoke(newObject, args);
                                        st.setString(1, name);
                                        st.setInt(2, age);
                                        st.setString(3, location);
                                        System.out.println("Adding new object to database... " + port);
                                        st.executeUpdate();
                                    }
                                    connection.commit();
                                    connection.setAutoCommit(true);
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                } catch (InstantiationException e) {
                                    e.printStackTrace();
                                }
                            }
                        }
                        if (!sentence.contains("collection")) {
                            for (int i = 0; i <= 10; i++) {
                                if (8880 + i != port) {
                                    needsRefreshing[i] = true;
                                }
                            }
                        }
                        System.out.println("Sending collection to client on port " + port);
                        System.out.println();
                        System.out.println();
                        TreeSet returnCollection = pullCollection(connection, humanBytes);
                        System.out.println("Test");
                        sendBuffer = ByteBuffer.wrap(serializeTree(returnCollection));
                        serverChannel.send(sendBuffer, clientAddress);
                        portQueue.poll();
                        receiveBuffer.clear();
                    }
                } catch (ClosedByInterruptException e) {
                    try {
                        if (portQueue.peek() == port) {
                            portQueue.poll();
                        }
                    } catch (NullPointerException np) {
                    }
                    try {
                        System.out.println("Port " + port + " is free now");
                        serverChannel.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                    monitorPort(port);
                } catch (IOException ex) {
                    ex.printStackTrace();
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        });
        serveThread.start();
    }

    public static SocketAddress receiveFromAddress(DatagramChannel serverChannel,SocketAddress client,ByteBuffer data) throws IOException {
        boolean received=false;
        SocketAddress thisClient=client;
        while(!received) {
            thisClient = serverChannel.receive(data);
            if (!getHostname(client).contains(getHostname(thisClient))) {
                data.clear();
                continue;
            }
            received=true;
        }
        return thisClient;
    }

    public static TreeSet<Object> pullCollection(Connection connection, byte[] A1){
        try {
            PreparedStatement st1 = connection.prepareStatement("select * from Humans;");
            ResultSet rs = st1.executeQuery();
            CachedRowSet cs = new CachedRowSetImpl();
            cs.populate(rs);
            TreeSet<Object> col = new TreeSet<Object>();
            while (cs.next()) {
                Class aclass = deserialize(A1).getClass();
                Object newObject = aclass.newInstance();
                Class[] paramTypes = new Class[] {String.class};
                Object[] args = new Object[] {cs.getString("name")};
                Method method1 = aclass.getMethod("setName", paramTypes);
                method1.invoke(newObject, args);
                Class[] paramTypes2 = new Class[] {int.class};
                Object[] args2 = new Object[] {cs.getInt("age")};
                Method method2 = aclass.getMethod("setAge", paramTypes2);
                method2.invoke(newObject, args2);
                Class[] paramTypes3 = new Class[] {String.class};
                Object[] args3 = new Object[] {cs.getString("location")};
                Method method3 = aclass.getMethod("setLocation", paramTypes);
                method3.invoke(newObject, args3);
                Class[] paramTypes4 = new Class[] {ZonedDateTime.class};
                Object[] args4 = new Object[] {ZonedDateTime.ofInstant(cs.getTimestamp("time").toInstant(), ZoneId.of("UTC"))};
                Method method4 = aclass.getMethod("setLastChangeTime", paramTypes4);
                method4.invoke(newObject, args4);
                col.add(newObject);
            }
            return col;
        }catch (SQLException e){
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String getHostname(SocketAddress address) {
        return ((InetSocketAddress) address).getHostName();
    }
}