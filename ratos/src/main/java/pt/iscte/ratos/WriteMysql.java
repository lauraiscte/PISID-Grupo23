package pt.iscte.ratos;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Random;
import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.text.BadLocationException;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

public class WriteMysql {

    static JTextArea documentLabelTemp = new JTextArea("\n");
    static JTextArea documentLabelMov = new JTextArea("\n");
    
    static Connection connTo;
    static String sql_database_connection_to = "";
    static String sql_database_password_to = "";
    static String sql_database_user_to = "";
    static String sql_table_to_temp = "";
    static String sql_table_to_mov = "";

    static String cloud_server = "";
    static String cloud_topic = "";
    static String cloud_topic1 = "";

    private static void createWindowTemp() {
        JFrame frame = new JFrame("Data Bridge");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Temperatura Data : ", SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane(documentLabelTemp, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }
        });
    }
    
    private static void createWindowMov() {
        JFrame frame = new JFrame("Data Bridge");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        JLabel textLabel = new JLabel("Passagens Data : ", SwingConstants.CENTER);
        textLabel.setPreferredSize(new Dimension(600, 30));
        JScrollPane scroll = new JScrollPane(documentLabelMov, JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
        scroll.setPreferredSize(new Dimension(600, 200));
        JButton b1 = new JButton("Stop the program");
        frame.getContentPane().add(textLabel, BorderLayout.PAGE_START);
        frame.getContentPane().add(scroll, BorderLayout.CENTER);
        frame.getContentPane().add(b1, BorderLayout.PAGE_END);
        frame.setLocationRelativeTo(null);
        frame.pack();
        frame.setVisible(true);
        b1.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent evt) {
                System.exit(0);
            }
        });
    }

    public static void main(String[] args) {
        createWindowTemp();
        createWindowMov();
        try {
            Properties p = new Properties();
            p.load(new FileInputStream("WriteMysql.ini"));
            sql_table_to_temp = p.getProperty("sql_table_to_temp");
            sql_table_to_mov = p.getProperty("sql_table_to_mov");
            sql_database_connection_to = p.getProperty("sql_database_connection_to");
            sql_database_password_to = p.getProperty("sql_database_password_to");
            sql_database_user_to = p.getProperty("sql_database_user_to");

            cloud_server = p.getProperty("cloud_server");
            cloud_topic = p.getProperty("mqtt_topic1");
            cloud_topic1 = p.getProperty("mqtt_topic2");
        } catch (Exception e) {
            System.out.println("Error reading WriteMysql.ini file " + e);
            JOptionPane.showMessageDialog(null, "The WriteMysql inifile wasn't found.", "Data Migration", JOptionPane.ERROR_MESSAGE);
        }
        new WriteMysql().connectDatabase_to();
        new WriteMysql().ReadData();
    }

    public void connectDatabase_to() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
            documentLabelTemp.append("SQl Connection:" + sql_database_connection_to + "\n");
            documentLabelTemp.append("Connection To MariaDB Destination " + sql_database_connection_to + " Suceeded" + "\n");
            documentLabelMov.append("SQl Connection:" + sql_database_connection_to + "\n");
            documentLabelMov.append("Connection To MariaDB Destination " + sql_database_connection_to + " Suceeded" + "\n");
        } catch (ClassNotFoundException e) {
            System.out.println("Driver JDBC não encontrado: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("Erro ao conectar ao banco de dados: " + e.getMessage());
        }
    }

    public void ReadData() {
        // Instanciação do objeto ReceiveCloud2 para conectar e receber os dados das clouds
        ReceiveCloud_Temp cloudReader_temp = new ReceiveCloud_Temp();
        cloudReader_temp.connecCloud(); // Conecta-se às clouds e começa a receber os dados

        ReceiveCloud_Mov cloudReader_mov = new ReceiveCloud_Mov();
        cloudReader_mov.connecCloud();

        // Aguarda um tempo para receber os dados das clouds
        try {
            Thread.sleep(60000); // Espera por 1 minuto (ou o tempo necessário)
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Obtenção dos dados lidos das clouds
        String dataFromCloudTemp = documentLabelTemp.getText(); // Dados da cloud pisid_grupo23_temp
        String dataFromCloudMov = documentLabelMov.getText();; // Dados da cloud pisid_grupo23_mov (você precisa implementar a lógica para obter esses dados)

        // Chama os métodos para inserir dados nas tabelas correspondentes
        WriteToMySQL(sql_table_to_temp, dataFromCloudTemp, "temperatura");
        WriteToMySQL(sql_table_to_mov, dataFromCloudMov, "porta");
    }

    public void WriteToMySQL(String table, String data, String tipoSensor) {
        try {
            Statement s = connTo.createStatement();
            // Verifica o tipo de sensor e constrói a instrução SQL de inserção apropriada
            if (tipoSensor.equals("temperatura")) {
                String[] tokens = data.split(",");
                String hora = tokens[0].substring(tokens[0].indexOf(":") + 1).trim();
                String leitura = tokens[1].substring(tokens[1].indexOf(":") + 1).trim();
                String sensor = tokens[2].substring(tokens[2].indexOf(":") + 1).trim();

                String sqlCommand = "INSERT INTO " + table + " (Hora, Leitura, IDSensor) VALUES ('" + hora + "', " + leitura + ", " + sensor + ")";
                int result = s.executeUpdate(sqlCommand);
            } else if (tipoSensor.equals("porta")) {
                String[] tokens = data.split(",");
                String hora = tokens[0].substring(tokens[0].indexOf(":") + 1).trim();
                String salaOrigem = tokens[1].substring(tokens[1].indexOf(":") + 1).trim();
                String salaDestino = tokens[2].substring(tokens[2].indexOf(":") + 1).trim();

                String sqlCommand = "INSERT INTO " + table + " (Hora, SalaOrigem, SalaDestino) VALUES ('" + hora + "', " + salaOrigem + ", " + salaDestino + ")";
                int result = s.executeUpdate(sqlCommand);
            }
            System.out.println("Data inserted into table " + table + " successfully");
            s.close();
        } catch (Exception e) {
            System.out.println("Error inserting data into table " + table + ": " + e);
        }
    }

    public class ReceiveCloud_Temp implements MqttCallback {
        MqttClient mqttclient;

        public void connecCloud() {
            int i;
            try {
                i = new Random().nextInt(100000);
                mqttclient = new MqttClient(cloud_server, "ReceiveCloud" + String.valueOf(i) + "_" + cloud_topic);
                mqttclient.connect();
                mqttclient.setCallback(this);
                mqttclient.subscribe(cloud_topic);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage c) throws Exception {
            try {
                documentLabelTemp.append(c.toString() + "\n");
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    }

    public class ReceiveCloud_Mov implements MqttCallback {
        MqttClient mqttclient;

        public void connecCloud() {
            int i;
            try {
                i = new Random().nextInt(100000);
                mqttclient = new MqttClient(cloud_server, "ReceiveCloud" + String.valueOf(i) + "_" + cloud_topic1);
                mqttclient.connect();
                mqttclient.setCallback(this);
                mqttclient.subscribe(cloud_topic1);
            } catch (MqttException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void messageArrived(String topic, MqttMessage c) throws Exception {
            try {
                documentLabelMov.append(c.toString() + "\n");
            } catch (Exception e) {
                System.out.println(e);
            }
        }

        @Override
        public void connectionLost(Throwable cause) {
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken token) {
        }
    }
}
