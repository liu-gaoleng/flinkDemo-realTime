import java.sql.*;
public class JdbcDemo {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/test?characterEncoding=utf8";
        String user = "root";
        String password = "123456";
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try{
            Class.forName("com.mysql.jdbc.driver").newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }




        try {
            conn  =  DriverManager.getConnection(url, user, password);
            stmt = conn.createStatement();
            queryAndDisplay(stmt, rs);
            stmt.executeUpdate("INSERT INTO person VALVES (9,  '刘文浩', 3, 'accountant', 2000, 4)");
            System.out.println("添加数据后的信息为：");
            queryAndDisplay(stmt, rs);

            stmt.executeUpdate("DELETE FROM person WHERE name = '刘文浩'");
            System.out.println("删除数据后的信息为：");
            queryAndDisplay(stmt, rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {try{rs.close();} catch (SQLException e) {e.printStackTrace();}}
            if (stmt != null) {try{stmt.close();} catch (SQLException e) {e.printStackTrace();}}
            if (conn != null) {try{conn.close();} catch (SQLException e) {e.printStackTrace();}}
        }
    }

    private static void queryAndDisplay(Statement stmt, ResultSet rs) throws SQLException{
        rs = stmt.executeQuery("select * from person");
        while (rs.next())
        {
            System.out.print(rs.getInt("id") +  " ");
            System.out.println(rs.getString("name") +  " ");
        }
    }
}