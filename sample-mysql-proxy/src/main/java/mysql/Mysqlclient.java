package mysql;

import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created with IntelliJ IDEA.
 * User: saikat
 * Date: 05/11/13
 * Time: 3:45 PM
 * To change this template use File | Settings | File Templates.
 */
public class MysqlClient {

    public MysqlClient() {
        ExecutorService executorService = Executors.newFixedThreadPool(1);
        for (int i=0; i < 1; i++) {

            executorService.execute(new Worker());
        }
        executorService.shutdown();
    }

    private class Worker implements Runnable {
        public void run() {
            Connection conn = null;
            try {
                String dburl = "jdbc:mysql://localhost:8080/mysql?characterEncoding=utf8&user=root&password=";
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                conn = DriverManager.getConnection(dburl);

                Statement s = conn.createStatement();
                ResultSet rs = s.executeQuery("select * from user");

                if(rs.next()){
                    System.out.println("host :" + rs.getString(1));
                }

                s.close();

            } catch(Exception e) {
                e.printStackTrace();
            }finally {
//                try {
//
//                    conn.close();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
            }

        }
    }

    public static void main(String[] args) {
        new MysqlClient();
    }
}
