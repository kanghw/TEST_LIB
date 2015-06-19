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
            
            // create table, delete table
//            String cteateTest = "create table test(id integer primary key AUTOINCREMENT,"
//                    + "name varchar(50) not null,"
//                    + "age int not null,"
//                    + "address char(50))";
//            String delTest = "drop table test";
//            
//            executeSQL(con, cteateTest);

            // insert
//            String insert = "insert into test(name, age, address) values(?, ?, ?)";
//            PreparedStatement pstmt = con.prepareStatement(insert);
//            pstmt.setString(1, "강현욱");
//            pstmt.setInt(2, 30);
//            pstmt.setString(3, "관악구");
//            pstmt.executeUpdate();
//            
//            pstmt.setString(1, "이정우");
//            pstmt.setInt(2, 33);
//            pstmt.setString(3, "하남");
//            pstmt.executeUpdate();

            // update
//            String updateSql = "update test set age = ? where name = ?";
//            PreparedStatement updatePstmt = con.prepareStatement(updateSql);
//            updatePstmt.setInt(1, 36);
//            updatePstmt.setString(2, "강현욱");
//            Integer updateResult = updatePstmt.executeUpdate();
//            System.out.println("update rows = " + updateResult);
            
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
            
            // delete
//            String deleteSql = "delete from test";
//            PreparedStatement deletePstmt = con.prepareStatement(deleteSql);
//            Integer deleteResult = deletePstmt.executeUpdate();
//            System.out.println("delete count = " + deleteResult);
            
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
    
    private static void executeSQL(Connection c, String sql){
        Statement stmt = null;
        try {
            stmt = c.createStatement();
            stmt.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
