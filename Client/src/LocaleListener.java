import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * Created by Денис on 12.06.2017.
 */
public class LocaleListener implements ActionListener{
    private LabFrame frame;
    private LabCollection collection;
    LocaleListener(LabFrame frame,LabCollection collection){
        super();
        this.frame=frame;
        this.collection=collection;
    }
    @Override
    public void actionPerformed(ActionEvent e) {
        JComboBox box=(JComboBox)e.getSource();
        switch((String)box.getSelectedItem()){
            case "Русский":{ConsoleApp.localization= ResourceBundle.getBundle("LocalizedResources",new Locale("ru","RU"));}break;
            case "Slovenský":{ConsoleApp.localization= ResourceBundle.getBundle("LocalizedResources",new Locale("sk","SK"));}break;
            case "Español":{ConsoleApp.localization= ResourceBundle.getBundle("LocalizedResources",new Locale("es","CR"));}break;
            case "Magyar":{ConsoleApp.localization= ResourceBundle.getBundle("LocalizedResources",new Locale("hu","HU"));}break;
        }
        ConsoleApp.repaintFrame(frame,collection);
    }
}
