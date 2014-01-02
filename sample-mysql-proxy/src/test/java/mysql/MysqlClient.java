package mysql;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
public class MysqlClient  {

    Connection conn = null;

    @Before
    public void setup() throws Exception{

        //create a mysql database mydb to test

        String dburl = "jdbc:mysql://localhost:8080/mydb?characterEncoding=utf8&user=root&password=";
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        conn = DriverManager.getConnection(dburl);
        Statement s = conn.createStatement();
        s.execute("create table mytable (tid INT NOT NULL AUTO_INCREMENT,t_title VARCHAR(100) NOT NULL,t_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY (tid))");

        PreparedStatement ps = conn.prepareStatement("delete from mytable");
        ps.execute();


        s.close();
        ps.close();

    }
    @Test
    public void runQueries() throws Exception{

        ExecutorService executorService = Executors.newFixedThreadPool(50);
        for (int i=0; i < 50; i++) {

            executorService.execute(new Worker());
        }
        executorService.shutdown();
    }

    @After
    public void teardown() throws Exception{
        Statement s = conn.createStatement();
        s.execute("drop table mytable");
        s.close();

    }

    private class Worker implements Runnable {
        public void run() {
            try {
                PreparedStatement ps = conn.prepareStatement("Insert into mytable(t_title,t_author,submission_date) values('mysql_proxy','saikat',now())");
                ps.execute();


                ps = conn.prepareStatement("select * from mytable where t_author='saikat'");
                ResultSet rs = ps.executeQuery();

                if(rs.next()){
                    Assert.assertEquals("mysql_proxy", rs.getString(2));
                    Assert.assertEquals("saikat",rs.getString(3));
                    Assert.assertNotNull(rs.getString(4));

                }

                ps = conn.prepareStatement("update mytable set t_author='dummy' where t_author='saikat'");
                ps.execute();

                ps = conn.prepareStatement("select * from mytable where t_author='dummy'");
                rs = ps.executeQuery();

                if(rs.next()){
                    Assert.assertEquals("mysql_proxy",rs.getString(2));
                    Assert.assertEquals("dummy",rs.getString(3));
                    Assert.assertNotNull(rs.getString(4));


                }
                rs.close();
                ps.close();

            } catch(Exception e) {
                e.printStackTrace();
            }

        }
    }
}
