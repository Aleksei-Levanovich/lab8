import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 * Created by aal on 13.06.2017.
 */
public class FilterListener implements ActionListener{

    @Override
    public void actionPerformed(ActionEvent e) {
        JFrame filter = new JFrame();
        JTextField field1 = new JTextField();
        field1.setPreferredSize(new Dimension(500,25));
        JTextField field2 = new JTextField();
        field2.setPreferredSize(new Dimension(500,25));
        JTextField field3=new JTextField();
        field3.setPreferredSize(new Dimension(500,25));
        JTextField field4= new JTextField();
        field4.setPreferredSize(new Dimension(500,25));
    }
}
