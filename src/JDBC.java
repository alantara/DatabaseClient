import java.sql.*;

public class JDBC {
    private Connection con = null;
    private Statement stm = null;
    public boolean connection_failed = false;

    public Connection getConnection(){
        return con;
    }

    public Statement getStatement(){
        return stm;
    }

    public void connect(DB_TYPE connType, String user, String password, String database_name){
        String url_conn;
        switch (connType){
            case POSTGRESQL -> url_conn = "jdbc:postgresql://localhost/" + database_name;
            case MYSQL -> url_conn = "jdbc:mysql://localhost/" + database_name;
            default -> url_conn = "";
        }

        try{
            con = DriverManager.getConnection(url_conn, user, password);
            stm = con.createStatement();
            connection_failed = false;
        } catch (SQLException err) {
            System.out.println("Error at connection creation: " + err);
            connection_failed = true;
        }
    }
}
