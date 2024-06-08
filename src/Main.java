import com.mysql.cj.MysqlType;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Scanner;

public class Main {
    private static JDBC db;

    private static String username;
    private static String password;
    private static String database_name="";
    private static DB_TYPE db_type;

    private static int maxRows = 1000;

    public static void main(String[] args) throws SQLException {
        db = new JDBC();

        do{
            username = getString("Enter database user: ");
            password = getString("Enter database password: ");
            db_type = DB_TYPE.MYSQL;

            db.connect(db_type, username, password, "");
        }while(db.connection_failed);

        System.out.println("Connected to database. To see a list of commands type \\h.");

        while (true){
            System.out.print("DatabaseClient:" + username + "@" + database_name + ">");
            String input = getString("");
            if(Objects.equals(input, "exit")){
                break;
            }

            String[] spl_input = input.split(" ");
            String command = spl_input[0];
            spl_input[0]="";
            String arguments = String.join(" ", spl_input).trim();

            switch (command){
                case "\\h" -> helpCommand();
                case "\\q" -> queryCommand(arguments);
                case "\\qs" -> querySaveCommand(arguments);
                case "\\tree" -> treeCommand();
                case "\\t" -> tablesCommand();
                case "\\i" -> tableInformationCommand(arguments);
                case "\\d" -> changeDatabaseCommand(arguments);
                case "\\m" -> changeMaxRowsCommand(arguments);
                default -> System.out.println("Specified command not found. Try \\h to see a list of commands.");
            }
        }
    }

    private static void helpCommand(){
        System.out.println("Here you can see a list of all usable commands");
        System.out.println("\t\\q [sql]   - execute a sql query in the selected database");
        System.out.println("\t\\qs [sql]  - execute a sql query in the selected database and save to file at ./[database_name]_[table].csv");
        System.out.println("\t\\tree      - shows tables and views of the current database in tree form");
        System.out.println("\t\\t         - shows tables and views of the current database");
        System.out.println("\t\\i [table] - shows table information");
        System.out.println("\t\\d [name]  - change to selected database");
        System.out.println("\t\\m [rows]  - change table display max rows");
        System.out.println("\texit       - terminate the connection");
    }

    private static void queryCommand(String query){
        String command = query.split(" ")[0].toLowerCase();
        switch (command){
            case "select", "explain" -> queryExecuteQuery(query);
            case "insert", "update", "delete" -> queryExecuteUpdate(query);
            default -> queryExecute(query);
        }
    }

    private static void querySaveCommand(String query){
        String command = query.split(" ")[0].toLowerCase();
        switch (command){
            case "select", "explain" -> queryExecuteQuerySave(query);
            default -> System.out.println("Error at saving response, SQL query not supported.");
        }
    }

    private static void queryExecuteQuery(String query){
        try{
            ResultSet rs = db.getStatement().executeQuery(query + " LIMIT " + maxRows);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            List<String> fields = CreateRowsList(rs);

            Table table = new Table(fields, columns);
            table.DrawTable();
            System.out.println((fields.size()/columns-1) + " rows in set");
            System.out.println("Display limited by " + maxRows + " rows. Call \\m [rows] to change this limit.");
        } catch (SQLException e){
            System.out.println("Error at query execution: " + e);
        }
    }

    private static void queryExecuteQuerySave(String query){
        try{
            ResultSet rs = db.getStatement().executeQuery(query);

            ResultSetMetaData rsmd = rs.getMetaData();
            int columns = rsmd.getColumnCount();
            List<String> fields = CreateRowsList(rs);

            File file = new File("./" + database_name + "_" + rsmd.getTableName(1) + ".csv");
            FileWriter outputfile = new FileWriter(file);
            CSVWriter writer = new CSVWriter(outputfile);
            for(int i = 0; i < fields.size()/columns; i++){
                String[] row = new String[columns];
                for(int j = 0; j < columns; j++){
                    row[j] = fields.get(j+columns*i);
                }
                writer.writeNext(row);
            }

            writer.close();
        } catch (SQLException | IOException e){
            System.out.println("Error at query execution: " + e);
        }
    }

    private static List<String> CreateRowsList(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columns = rsmd.getColumnCount();
        List<String> fields = new ArrayList<String>();

        for(int i = 1; i <= columns; i++){
            fields.add(rsmd.getColumnName(i));
        }

        while(rs.next()){
            for(int i = 1; i <= columns; i++){
                switch (rsmd.getColumnType(i)){
                    case Types.INTEGER, Types.DECIMAL -> {
                        int field = rs.getInt(i);
                        fields.add(String.valueOf(field));
                    }
                    case Types.VARCHAR -> {
                        String field = rs.getString(i);
                        fields.add(field);
                    }
                }
            }
        }
        return fields;
    }

    public static void queryExecuteUpdate(String query){
        try {
            int rows = db.getStatement().executeUpdate(query);
            System.out.println("Query OK, " + rows + " row affected");
        } catch (SQLException e) {
            System.out.println("Error at query execution: " + e);
        }
    }

    public static void queryExecute(String query){
        try {
            db.getStatement().execute(query);
        } catch (SQLException e) {
            System.out.println("Error at query execution: " + e);
        }
    }

    public static void treeCommand(){
        try {
            DatabaseMetaData md = db.getConnection().getMetaData();
            ResultSet rs = md.getTables(database_name, null, "%", null);

            while(rs.next()){
                String table_name = rs.getString(3);
                ResultSet rscol = md.getColumns(database_name, null, table_name, null);
                List<String> pks = new ArrayList<String>();

                ResultSet pkrs = md.getPrimaryKeys(database_name, null, table_name);
                while(pkrs.next()){
                    pks.add(pkrs.getString(4));
                }

                System.out.println(table_name);

                while(rscol.next()){
                    String name = rscol.getString(4);
                    String type = rscol.getString(6);
                    int size = rscol.getInt(7);
                    boolean pk = pks.contains(name);
                    System.out.println("\t" + name + " " + type + "(" + size + ") " + ((pk) ? "(PK)": ""));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tablesCommand(){
        try {
            DatabaseMetaData md = db.getConnection().getMetaData();
            ResultSet rs = md.getTables(database_name, null, "%", null);

            List<String> fields = new ArrayList<String>();
            fields.add("tables");
            while(rs.next()){
                String table_name = rs.getString(3);
                fields.add(table_name);
            }
            Table table = new Table(fields, 1);
            table.DrawTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void tableInformationCommand(String table_name){
        try {
            DatabaseMetaData md = db.getConnection().getMetaData();
            ResultSet rs = md.getColumns(database_name, null, table_name, null);

            List<String> fields = new ArrayList<String>();
            fields.add("column_name");
            fields.add("column_type");
            fields.add("column_size");
            fields.add("column_PK");

            List<String> pks = new ArrayList<String>();
            ResultSet pkrs = md.getPrimaryKeys(database_name, null, table_name);
            while(pkrs.next()){
                pks.add(pkrs.getString(4));
            }

            while(rs.next()){
                String name = rs.getString(4);
                String type = rs.getString(6);
                int size = rs.getInt(7);
                boolean pk = pks.contains(name);

                fields.add(name);
                fields.add(type);
                fields.add(String.valueOf(size));
                fields.add((pk) ? "true" : "false");
            }
            Table table = new Table(fields, 4);
            table.DrawTable();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void changeDatabaseCommand(String new_database){
        db.connect(db_type, username, password, new_database);
        if(!db.connection_failed){
            database_name = new_database;
        }
    }

    public static void changeMaxRowsCommand(String new_max){
        maxRows = Integer.parseInt(new_max);
    }

    public static String getString(String msg){
        Scanner scanner = new Scanner(System.in);

        System.out.print(msg);
        return scanner.nextLine().trim();
    }
}