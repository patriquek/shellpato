/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Database;

import Messaging.LogWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import javax.ejb.Asynchronous;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

/**
 *
 * @author manmaish
 */
@Asynchronous
public class DBConn {

    public Connection conn = null;
    public Statement stmt = null;
    public ResultSet rs = null;
    public Context ctx = null;

    public DBConn() {

    }
@Asynchronous
    public final Connection dbConn() {
        try {
            DataSource ds = null;
            InitialContext ic = new InitialContext();
            ds = (DataSource) ic.lookup("java:/datasources/mypool");
            conn = ds.getConnection();            
        } catch (NamingException | SQLException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("ESB_ERROR_MDB");
        }
        return conn;
    }
@Asynchronous
    public void close() {
        try {
            //Close JDBC objects as soon as possible
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
            if (ctx != null) {
                ctx.close();
            }
        } catch (SQLException | NamingException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("ESB_ERROR_MDB");
        }
    }
}
