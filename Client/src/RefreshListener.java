import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.TreeSet;

/**
 * Created by Денис on 05.06.2017.
 */
public class RefreshListener implements ActionListener {
    LabTable collection;
    RefreshListener(LabTable colTable) {
        super();
        collection=colTable;
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        Human random = new Human();
        collection.getHumans().clear();
        collection.getHumans().addAll(LabListener.makeCall("collection",random).getUselessData());
        collection.fireTableDataChanged();
    }
}
