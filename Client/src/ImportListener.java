import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.DatagramChannel;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Денис on 25.04.2017.
 */
public class ImportListener extends LabListener {
    JProgressBar jpb1;

    ImportListener(JTextField field, TreeSet<Human> col, LabTable colTable, JProgressBar jpb) {
        super(field, col, colTable, jpb);
        jpb1 = jpb;
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        ProgressBarThread jPBarThread = new ProgressBarThread(jpb1);
        jPBarThread.start();
        new Thread(new Runnable() {
            @Override
            public void run() {
                Matcher m = Pattern.compile("\\{([^}]+)\\}").matcher(getNameField().getText());
                TreeSet<Human> ImportCol = new TreeSet<>();
                while (m.find()) {
                    //getCollection().addAll(makeCall(m.group().substring(1, m.group().length() - 1)).getUselessData());
                    //getTable().fireTableDataChanged();
                    ImportCol = ConsoleApp.ImportFrom(m.group().substring(1, m.group().length() - 1)).getUselessData();
                    getNameField().setText("");
                    getCollection().clear();
                    getCollection().addAll(makeCall("import",ImportCol).getUselessData());
                    getTable().fireTableDataChanged();
                }

            }
        }).start();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e1) {
            e1.printStackTrace();
        }
        jPBarThread.interrupt();
    }

    protected LabCollection makeCall(String command, TreeSet<Human> ImportCollection) {
        try {
            SocketAddress address = new InetSocketAddress(ConsoleApp.HOSTNAME, ConsoleApp.port);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData;
            byte[] receiveData = new byte[1024];
            byte[] refreshFlag = new byte[1024];
            String sentence = command;
            sendData = sentence.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            DatagramPacket flagPacket = new DatagramPacket(refreshFlag, refreshFlag.length);
            clientSocket.send(sendPacket);
            clientSocket.send(new DatagramPacket(serialize(ImportCollection), serialize(ImportCollection).length, address));
            clientSocket.receive(flagPacket);
            clientSocket.receive(receivePacket);
            String flag = new String(refreshFlag);
            if (flag.contains("true")) {
                System.out.print(ConsoleApp.localization.getString("oldDataError"));
                String path = "src/music/shekh.wav";
                MusicRunnable t1 = new MusicRunnable();
                t1.path = path;
                Thread thread = new Thread(t1);
                thread.start();
            }
            ConsoleApp.timeOut.interrupt();
            LabCollection receivedCollection = LabCollection.deserialize(receivePacket.getData());
            clientSocket.close();
            return receivedCollection;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public byte[] serialize(TreeSet<Human> eee) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(eee);
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
}
