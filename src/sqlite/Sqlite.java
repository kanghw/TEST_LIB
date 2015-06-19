package sqlite;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Sqlite {
    private static String DB_NAME = "jdbc:sqlite:test.db";
    
    public static void main(String[] args) {
        Connection con = null;
        try {
            con = connectDB();
            
            // create table
            String cteateTable = "create table test(id integer primary key AUTOINCREMENT,"
                    + "name varchar(50) not null,"
                    + "age int not null,"
                    + "address char(50))";
            Integer createResult = executeSQL(con, cteateTable);
            System.out.println("create table result = " + createResult);

            // insert
            String insert = "insert into test(name, age, address) values(?, ?, ?)";
            PreparedStatement insertPstmt = con.prepareStatement(insert);
            insertPstmt.setString(1, "whoami");
            insertPstmt.setInt(2, 30);
            insertPstmt.setString(3, "whereami");
            executeUpdate(insertPstmt);
            
            insertPstmt = con.prepareStatement(insert);
            insertPstmt.setString(1, "whoareyou");
            insertPstmt.setInt(2, 33);
            insertPstmt.setString(3, "whereareyou");
            executeUpdate(insertPstmt);

            // update
            String updateSql = "update test set age = ? where name = ?";
            PreparedStatement updatePstmt = con.prepareStatement(updateSql);
            updatePstmt.setInt(1, 36);
            updatePstmt.setString(2, "whoami");
            Integer updateResult = executeUpdate(updatePstmt);
            System.out.println("update rows = " + updateResult);
            
            // select
            String selectSql = "select * from test";
            PreparedStatement selectPstmt = con.prepareStatement(selectSql);
            ResultSet rs = selectPstmt.executeQuery();
            while(rs.next()){
                int id = rs.getInt("id");
                String name = rs.getString("name");
                int age = rs.getInt("age");
                String address = rs.getString("address");
                
                System.out.println("id = " + id + ", name = " + name + ", age = " + age + ", address = " + address);
            }
            rs.close();
            selectPstmt.close();
            
            // delete
            String deleteSql = "delete from test";
            PreparedStatement deletePstmt = con.prepareStatement(deleteSql);
            Integer deleteResult = executeUpdate(deletePstmt);
            System.out.println("delete count = " + deleteResult);
            
            // delete table
            String deleteTable = "drop table test";
            Integer dropResult = executeSQL(con, deleteTable);
            System.out.println("drop table result = " + dropResult);
            
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                con.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        System.out.println("-------------------------------------------------------------END");
    }
    
    private static Connection connectDB() throws ClassNotFoundException, SQLException{
        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(DB_NAME); 
    }
    
    private static Integer executeSQL(Connection c, String sql){
        Integer result = null;
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            result = stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
    
    private static Integer executeUpdate(PreparedStatement pstmt){
        Integer result = null;
        try {
            result = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return result;
    }
}
