package test.impala;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UpdateMeta
{
	//10.1.16.39:21000
    static String JDBC_DRIVER = "com.cloudera.impala.jdbc41.Driver";
    static String CONNECTION_URL = "jdbc:impala://10.1.16.37:21050/obd_message";

    public static void main(String[] args)
    {
        Connection con = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try
        {
            Class.forName(JDBC_DRIVER);
            con = DriverManager.getConnection(CONNECTION_URL);
            ps = con.prepareStatement("select * from gps_log_last limit 10");
            rs = ps.executeQuery();
            while (rs.next())
            {
                System.out.println(rs.getString(1)+ "----");
            }
        } catch (Exception e)
        {
            e.printStackTrace();
        } 
        finally{
        	//关闭rs、ps和con
        	try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        	try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

        	try {
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        
        	
        }
    }
}