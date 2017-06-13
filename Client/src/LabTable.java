import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableModel;
import java.io.*;
import java.lang.reflect.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Created by Денис on 25.04.2017.
 */
public class LabTable extends AbstractTableModel implements TableModel {

    private TreeSet<Human> Humans;

    LabTable(TreeSet<Human> Humans) {
        super();
        this.Humans = Humans;
    }

    @Override
    public boolean isCellEditable(int rowIndex, int columnIndex) {
        if (columnIndex != 0) {
            return true;
        } else return false;
    }

    @Override
    public String getColumnName(int column) {
        String result = "";
        switch (column) {
            case 0:
                result = "№";
                break;
            case 1:
                result = ConsoleApp.localization.getString("Column1");
                break;
            case 2:
                result = ConsoleApp.localization.getString("Column2");
                break;
            case 3:
                result = ConsoleApp.localization.getString("Column3");
                break;
            case 4:
                result = ConsoleApp.localization.getString("Column4");;
        }
        //  case 3:return("Gender");
        return result;
    }

    @Override
    public int getRowCount() {
        return Humans.size();
    }

    @Override
    public void setValueAt(Object aValue, int rowIndex, int columnIndex) {
        if (aValue != null && aValue.toString().length() > 0) {
            Human[] arr = Humans.toArray(new Human[Humans.size()]);
            Human object = arr[(int) getValueAt(rowIndex, 0) - 1];
            switch (columnIndex) {
                case 1:
                    if (Pattern.compile("[A-zА-я]+").matcher(aValue.toString()).matches()) {
                        getHumans().clear();
                        getHumans().addAll(makeUpdateCall(object, 1, (String) aValue).getUselessData());
                            /*Humans.remove(arr[(int) getValueAt(rowIndex, 0) - 1]);
                            Humans.add(new Human((String) aValue, arr[(int) getValueAt(rowIndex, 0) - 1].getAge(), arr[(int) getValueAt(rowIndex, 0) - 1].getLocation()));*/
                    } else {
                        System.out.print(ConsoleApp.localization.getString("error2"));
                    }
                    break;
                case 2:
                    if ((int) aValue >= 0 && (int) aValue <= 120) {
                        getHumans().clear();
                        getHumans().addAll(makeUpdateCall(object, 2, Integer.toString((int) aValue)).getUselessData());
                            /*Humans.remove(arr[(int) getValueAt(rowIndex, 0) - 1]);
                            Humans.add(new Human(arr[(int) getValueAt(rowIndex, 0) - 1].getName(), (int) aValue, arr[(int) getValueAt(rowIndex, 0) - 1].getLocation()));*/
                    } else {
                        System.out.print(ConsoleApp.localization.getString("error3"));
                    }
                    break;
                case 3:
                    if (Pattern.compile("[A-zА-я0-9\\-_]+").matcher(aValue.toString()).matches()) {
                        getHumans().clear();
                        getHumans().addAll(makeUpdateCall(object, 3, (String) aValue).getUselessData());
                            /*Humans.remove(arr[(int) getValueAt(rowIndex, 0) - 1]);
                            Humans.add(new Human(arr[(int) getValueAt(rowIndex, 0) - 1].getName(), arr[(int) getValueAt(rowIndex, 0) - 1].getAge(), (String) aValue));*/
                    } else {
                        System.out.print(ConsoleApp.localization.getString("error1"));
                    }
                    break;
            }
            fireTableDataChanged();
        }
    }

    @Override
    public Object getValueAt(int rowIndex, int columnIndex) {
        int i = 0;
        Human[] arr = new Human[Humans.size()];
        for (Human a : Humans) {
            arr[i] = new Human();
            arr[i].setAge(a.getAge());
            arr[i].setName(a.getName());
            arr[i].setLocation(a.getLocation());
            arr[i].setLastChangeTime(a.getLastChangeTime());
            i++;
        }
        if (getHumans().size() != 0)
            switch (columnIndex) {
                case 0: {
                    return rowIndex + 1;
                }
                case 1: {
                    return arr[rowIndex].getName();
                }
                case 2: {
                    return arr[rowIndex].getAge();
                }
                case 3: {
                    return arr[rowIndex].getLocation();
                }
                case 4: {
                    return DateTimeFormatter.ofPattern("dd/MM/yyyy - HH:mm").format(arr[rowIndex].getLastChangeTime().withZoneSameInstant(ZoneId.of(ConsoleApp.localization.getString("timeZone"))));
                }
                default: {
                    return null;
                }
            }
        else return (null);
    }

    @Override
    public int getColumnCount() {
        return 5; //4;
    }

    @Override
    public Class<?> getColumnClass(int columnIndex) {
        Class returnValue;
        switch (columnIndex) {
            case 0:
                returnValue = Integer.class;
                break;
            case 1:
                returnValue = String.class;
                break;
            case 2:
                returnValue = Integer.class;
                break;
            case 3:
                returnValue = String.class;
                break;
            case 4:
                returnValue = String.class;
                break;
            default:
                returnValue = String.class;
                break;
        }
        return returnValue;
    }

    public TreeSet<Human> getHumans() {
        return Humans;
    }

    protected LabCollection makeUpdateCall(Human object, int attributeNumber, String attributeValue) {
        try {
            SocketAddress address = new InetSocketAddress(ConsoleApp.HOSTNAME, ConsoleApp.port);
            DatagramSocket clientSocket = new DatagramSocket();
            byte[] sendData;
            byte[] receiveData = new byte[8192];
            byte[] refreshFlag = new byte[8192];
            sendData = "update".getBytes();
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address);
            clientSocket.send(sendPacket);
            sendData = object.serialize();
            sendPacket = new DatagramPacket(sendData, sendData.length, address);
            clientSocket.send(sendPacket);
            sendData = ByteBuffer.allocate(4).putInt(attributeNumber).array();
            clientSocket.send(new DatagramPacket(sendData, sendData.length, address));
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            try {
                clientSocket.send(new DatagramPacket(attributeValue.getBytes(), attributeValue.getBytes().length, address));
            } catch (IOException ex) {
                // ignore close exception
            } finally {
                try {
                    bos.close();
                } catch (IOException e) {
                }
            }
            DatagramPacket flagPacket = new DatagramPacket(refreshFlag, refreshFlag.length);
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            clientSocket.receive(flagPacket);
            clientSocket.receive(receivePacket);
            String flag = new String(refreshFlag);
            if (flag.contains("true")) {
                System.out.print(ConsoleApp.localization.getString("oldDataError"));
            }
            clientSocket.close();

            return (LabCollection.deserialize(receivePacket.getData()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private Object deserialize(byte[] data) {
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            Object o = in.readObject();
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
}