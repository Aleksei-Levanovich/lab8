/**
 * Created by Денис on 02.03.2017.
 */

import javax.imageio.ImageIO;
import javax.swing.*;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableRowSorter;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.*;
import java.net.*;
import java.util.ResourceBundle;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Thread.currentThread;

public class ConsoleApp {
    volatile  static boolean timedOut=false;
    final static String defaultPath = "src/Input_Output_Files/input.txt";
    static final String HOSTNAME = "52.174.197.43";
    static final String USERNAME = "kjkszpj361";
    static final String PASSWORD = "B9zbYEl*dj}6";
    static TimeoutThread timeOut;
    static public int port=8880;
    static ResourceBundle localization=ResourceBundle.getBundle("LocalizedResources");
    public static void main(String[] args) {
        timeOut=new TimeoutThread(Thread.currentThread());
        timeOut.start();
        LabCollection ExpCol = tryToConnect();
        if (ExpCol!=null) {
            SwingUtilities.invokeLater(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                gui(ExpCol);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    });

            Runtime.getRuntime().addShutdownHook(new Thread() {
                                                     public void run() {
                                                         if (timedOut) tryToConnect();
                                                         makeCall("disconnect", new Human("asldn", 12, "msklad"));
                                                         SaveCollection(ExpCol);
                                                     }
                                                 }
            );
        }
    }

    private static void gui(LabCollection labCollection) throws InterruptedException {
        localization= ResourceBundle.getBundle("LocalizedResources");
        LabFrame guiFrame = new LabFrame(localization.getString("windowTitle"));
        repaintFrame(guiFrame,labCollection);
        guiFrame.setVisible(true);
    }

    public static void repaintFrame(LabFrame guiFrame,LabCollection labCollection){
        guiFrame.getContentPane().removeAll();
        //  <Background setting>
        JLabel backLabel=new JLabel();
        try {
            backLabel=new JLabel(new ImageIcon(ImageIO.read(new File("src/Backgrounds/animebg.jpg"))));
            guiFrame.setContentPane(backLabel);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //  </Background setting>

        //  <Frame Setting>
        guiFrame.setSize(720, 830);
        guiFrame.setResizable(false);
        guiFrame.setLayout(new FlowLayout());
        guiFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        //  </Frame Setting>

        //  <Table setting>
        guiFrame.setTable(new LabTable(labCollection.getUselessData()));
        //  <Table sorter setting>
        TableRowSorter<LabTable> sort = new TableRowSorter<LabTable>(guiFrame.getTable());
        JTable sortTable = new JTable(guiFrame.getTable());
        DefaultTableCellRenderer first = new DefaultTableCellRenderer();
        first.setHorizontalAlignment(JLabel.CENTER);
        sortTable.getColumnModel().getColumn(0).setPreferredWidth(15);
        sortTable.getColumnModel().getColumn(1).setPreferredWidth(80);
        sortTable.getColumnModel().getColumn(2).setPreferredWidth(15);
        sortTable.getColumnModel().getColumn(3).setPreferredWidth(80);
        sortTable.getColumnModel().getColumn(4).setPreferredWidth(140);
        sortTable.getColumnModel().getColumn(0).setCellRenderer(first);
        sortTable.getColumnModel().getColumn(1).setCellRenderer(first);
        sortTable.getColumnModel().getColumn(2).setCellRenderer(first);
        sortTable.getColumnModel().getColumn(3).setCellRenderer(first);
        sortTable.getColumnModel().getColumn(4).setCellRenderer(first);
        sortTable.setRowSorter(sort);
        // </Table sorter setting>

        // <Table filter setting>
        JTextField filterText = new JTextField(30);
        JButton filterButton = new JButton(localization.getString("filterButton"));
        filterText.addKeyListener(new EnterListener(filterButton));
        filterButton.setPreferredSize(new Dimension(125,20));
        // <Table filter setting>

        //  </Table setting>

        //  <Output panel Setting>
        JTextPane OutputPanel = new JTextPane();
        OutputPanel.setPreferredSize(new Dimension(690, 135));
        OutputPanel.setText(ConsoleApp.localization.getString("Hello"));
        OutputPanel.setEditable(false);
        TextPaneOutputStream outputStream=new TextPaneOutputStream(OutputPanel);
        System.setOut(new PrintStream(outputStream));
        //  </Output panel setting>

        //  <ProgressBar setting>
        JProgressBar jpb = new JProgressBar();
        jpb.setPreferredSize(new Dimension(690, 35));
        jpb.setIndeterminate(false);
        //  </ProgressBar setting>

        //  <"SingleAdd" button setting>
        SingleAddingListener addingListener=new SingleAddingListener(new JTextField(),new JSpinner(),new JTextField(),labCollection.getUselessData(),guiFrame.getTable(),jpb);
        LabButton addingButton = new LabButton(localization.getString("addButton"),addingListener);
        //  </"SingleAdd" button setting>
        RefreshListener refreshListener = new RefreshListener(guiFrame.getTable());
        JButton refresh = new JButton(localization.getString("refreshButton"));
        refresh.addActionListener(refreshListener);
        refresh.setPreferredSize(new Dimension(100,20));
        //  <"Remove" button setting>
        RemoveListener rmListener=new RemoveListener(new JTextField(),new JSpinner(),new JTextField(),labCollection.getUselessData(),guiFrame.getTable(),jpb,backLabel);
        LabButton rmButton = new LabButton(localization.getString("removeButton"),rmListener);
        //  </"Remove" button setting>

        //  <"Remove lower" button setting>
        RemoveLowerListener rmlListener=new RemoveLowerListener(new JTextField(),new JSpinner(),new JTextField(),labCollection.getUselessData(),guiFrame.getTable(),jpb);
        LabButton rmLButton = new LabButton(localization.getString("removeLowerButton"), rmlListener);
        //  </"Remove lower" button setting>

        //  <"Import" button setting>
        LabButton ImportButton = new LabButton(localization.getString("importButton"),labCollection.getUselessData(), guiFrame.getTable(),new JTextField(),jpb);
        //  </"Import" button setting>

        //  <"Save" button setting>
        JButton saveButton = new JButton(localization.getString("saveButton"));
        saveButton.setPreferredSize(new Dimension(125,20));
        saveButton.addActionListener(new SaveListener(labCollection));
        //  </"Save" button setting>

        String[] locales={"Русский","Slovenský","Español","Magyar"};
        JComboBox localeBox=new JComboBox(locales);
        switch (localization.getString("loc")){
            case "RU":localeBox.setSelectedIndex(0); break;
            case "SK":localeBox.setSelectedIndex(1);break;
            case "ES":localeBox.setSelectedIndex(2);break;
            case "HU":localeBox.setSelectedIndex(3);break;
        }
        localeBox.addActionListener(new LocaleListener(guiFrame,labCollection));

        //  <Adding elements to frame>
        guiFrame.add(rmLButton.getButtonPanel());
        guiFrame.add(rmButton.getButtonPanel());
        guiFrame.add(addingButton.getButtonPanel());
        guiFrame.add(ImportButton.getButtonPanel());
        guiFrame.add(refresh);
        guiFrame.add(saveButton);
        guiFrame.add(new JScrollPane(sortTable));
        //guiFrame.add(spinPanel);
        guiFrame.add(localeBox);
        guiFrame.add(new JScrollPane(OutputPanel));
        guiFrame.add(jpb);
        //  </Adding elements to frame>
        guiFrame.invalidate();
        guiFrame.validate();
        guiFrame.repaint();
    }

    public static LabCollection ImportFrom(String path) {
        LabCollection a = new LabCollection();
        try {
            File input = new File(path);
            Scanner startReader = new Scanner(input);
            String currentString = startReader.nextLine();
            a = new LabCollection(currentString.split(" ")[1].substring(6, currentString.split(" ")[1].length() - 2));
            currentString = startReader.nextLine();
            while (!currentString.contains("</Collection>")) {
                String[] sepString = currentString.split(" ");
                if (sepString[0].matches("\t<Human")) {
                    String name = null;
                    boolean[] hasAttributes = new boolean[3];
                    int age = -1;
                    String location = null;
                    Matcher forHuman = Pattern.compile("[A-z]+=\"[A-z,А-я,0-9]+\"").matcher(currentString);
                    while (forHuman.find()) {
                        String[] buffString = forHuman.group().split("=");
                        switch (buffString[0]) {
                            case "name":
                                Matcher hName = Pattern.compile("[0-9]").matcher(buffString[1]);
                                if (!hName.find()) {
                                    name = buffString[1].substring(1, buffString[1].length() - 1);
                                    hasAttributes[0] = true;
                                }
                                break;
                            case "age":
                                Matcher hAge = Pattern.compile("\"[0-9]+\"").matcher(buffString[1]);
                                if (hAge.matches()) {
                                    age = Integer.parseInt(buffString[1].substring(1, buffString[1].length() - 1));
                                    hasAttributes[1] = true;
                                }
                                break;
                            case "loc":
                                location = buffString[1].substring(1, buffString[1].length() - 1);
                                hasAttributes[2] = true;
                                break;
                        }
                    }
                    if (hasAttributes[0] && hasAttributes[1] && hasAttributes[2]) {
                        Human newHuman=new Human(name, age, location);
                        a.getUselessData().add(newHuman);
                    }
                }
                currentString = startReader.nextLine();
            }
        } catch (FileNotFoundException e) {
            System.out.print(localization.getString("FnFError"));
        }
        return a;
    }


    public static void SaveCollection(LabCollection collection) {
        try {
            FileWriter toEmptyFile = new FileWriter(defaultPath);
            BufferedWriter headerWriter = new BufferedWriter(toEmptyFile);
            headerWriter.write("<Collection name=\"" + collection.getName() + "\">");
            headerWriter.close();
            FileWriter fileWriter = new FileWriter(defaultPath, true);
            BufferedWriter writer = new BufferedWriter(fileWriter);
            writer.newLine();
            for (Comparable elem : collection.getUselessData()) {
                String[] a = elem.toString().split(" ");
                switch (elem.toString().charAt(0)) {
                    case 'Ч': {
                        writer.write("\t<Human name=\"" + a[3] + "\" age=\"" + a[5] + "\" loc=\"" + a[9] + "\"/>");
                        writer.newLine();
                        break;
                    }
                }
                writer.flush();
            }
            writer.write("</Collection>");
            writer.close();
            System.out.print(ConsoleApp.localization.getString("Saved")+defaultPath);
        } catch (Exception f) {
            System.out.print(f.getMessage());
        }
    }

    public static LabCollection tryToConnect() {
        int i=0;
        try {
            for (i = 8880; i <= 8890; i++) {
                System.out.println(ConsoleApp.localization.getString("TtC") + i);
                DatagramSocket clientSocket = new DatagramSocket();
                InetSocketAddress address = new InetSocketAddress(HOSTNAME, i);
                DatagramPacket testPacket = new DatagramPacket("test".getBytes(), "test".getBytes().length, address);
                clientSocket.setSoTimeout(1000);
                byte[] receiveData = new byte[8192];
                DatagramPacket testReceivePacket = new DatagramPacket(receiveData, receiveData.length);
                try {
                    clientSocket.send(testPacket);
                    clientSocket.receive(testReceivePacket);
                    port=i;
                    break;
                }catch (SocketTimeoutException ex){System.out.println(ConsoleApp.localization.getString("Port")+i+ConsoleApp.localization.getString("Unavailable"));continue;}
            }
        } catch (Exception e) {
            System.out.println("asda");
            e.printStackTrace();
        }
        if(i==8891){System.out.print(ConsoleApp.localization.getString("CannotReachServer")); System.exit(1); return null;}
        else{Human A = new Human("Alex", 19, "A");
            return LabListener.makeCall("collection",A);
        }
    }

    public static void makeCall(String command,Human object) {
        try {
            SocketAddress address = new InetSocketAddress(ConsoleApp.HOSTNAME, ConsoleApp.port);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData;
            byte[] receiveData = new byte[8192];
            String sentence = command;
            sendData = sentence.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address);
            clientSocket.send(sendPacket);
            clientSocket.send(new DatagramPacket(object.serialize(), object.serialize().length, address));
        } catch (IOException e) {
        }
    }
}

