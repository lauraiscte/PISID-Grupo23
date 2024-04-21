package pt.iscte.ratos;

import java.io.*;
import java.util.*;
import java.util.List;
import java.text.SimpleDateFormat;  
import java.util.Date;  
import java.util.concurrent.*;
import java.sql.*;
import javax.swing.*;
import java.awt.event.*;
import java.awt.*; 
import javax.swing.text.BadLocationException;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class WriteMysql {	
	static JTextArea documentLabel = new JTextArea("\n");
	static Connection connTo;
	static String sql_database_connection_to = new String();
	static String sql_database_password_to= new String();
	static String sql_database_user_to= new String();
	static String sql_table_to_temp = new String();
	static String sql_table_to_mov = new String();
	
	static String cloud_server = new String();
    static String cloud_topic = new String();
    static String cloud_topic1 = new String();
		
	private static void createWindow() {   
		JFrame frame = new JFrame("Data Bridge");    
		frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);       
		JLabel textLabel = new JLabel("Data : ",SwingConstants.CENTER);       
		textLabel.setPreferredSize(new Dimension(600, 30));   	
		JScrollPane scroll = new JScrollPane (documentLabel,JScrollPane.VERTICAL_SCROLLBAR_ALWAYS, JScrollPane.HORIZONTAL_SCROLLBAR_ALWAYS);
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
	
	public  static void main(String[] args) {
		createWindow();
		try {	
          Properties p = new Properties();
          p.load(new FileInputStream("WriteMysql.ini"));			
			sql_table_to_temp= p.getProperty("sql_table_to_temp");
			sql_table_to_mov= p.getProperty("sql_table_to_mov");
			sql_database_connection_to = p.getProperty("sql_database_connection_to");
			sql_database_password_to = p.getProperty("sql_database_password_to");
			sql_database_user_to= p.getProperty("sql_database_user_to");
			
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
			connTo =  DriverManager.getConnection(sql_database_connection_to,sql_database_user_to,sql_database_password_to);
			documentLabel.append("SQl Connection:"+sql_database_connection_to+"\n");				
			documentLabel.append("Connection To MariaDB Destination " + sql_database_connection_to + " Suceeded"+"\n");	
	   } catch (Exception e){System.out.println("Mysql Server Destination down, unable to make the connection. "+e);}
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
	    String dataFromCloudTemp = cloudReader_temp.documentLabel.getText(); // Dados da cloud pisid_grupo23_temp
	    String dataFromCloudMov = cloudReader_mov.documentLabel.getText();; // Dados da cloud pisid_grupo23_mov (você precisa implementar a lógica para obter esses dados)

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

	
}