/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Database;

import Messaging.LogWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import javax.ejb.Asynchronous;
import oracle.jdbc.OracleTypes;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author manmaish
 */
@Asynchronous
public class DBFunctions {

    /**
     *
     * @param AccountNumber
     * @return
     */
    @Asynchronous
    public String getaccountbalance(String AccountNumber) {
        String Accountbalance = "";
        DBConn DB = new DBConn();
        try (Connection conn = DB.dbConn();
                CallableStatement statement = conn.prepareCall("{? = call FN_GET_AC_BAL(?,?)}")) {

            statement.registerOutParameter(1, java.sql.Types.VARCHAR);
            statement.setString(2, AccountNumber);//V_ACCOUNT_NO
            statement.setString(3, "wallet");//V_CBS
            statement.execute(); // Execute the callable statement
            Accountbalance = statement.getString(1);

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        } finally {
            DB.close();
        }
        return Accountbalance;
    }
@Asynchronous
    public boolean insertmessage(JSONObject data) {
        boolean successful = false;
        DBConn DB = new DBConn();
        String SP_SQL = "insert into tbmessages  "
                + " (direction, channel_timestamp, channel_reference, channel_ip, geolocation, user_agent, user_agent_version, "
                + "channel, transaction_type, transaction_code, host_code, client_id,  debit_account, phone_number,esb_reference)values( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        try (Connection connection = DB.dbConn();
                PreparedStatement statement = connection.prepareStatement(SP_SQL)) {
            statement.setString(1, data.getJSONObject("data").getJSONObject("transaction_details").getString("direction"));
            statement.setString(2, data.getString("txntimestamp"));
            statement.setString(3, data.getString("xref"));
            statement.setString(4, data.getJSONObject("data").getJSONObject("channel_details").getString("host"));
            statement.setString(5, data.getJSONObject("data").getJSONObject("channel_details").getString("geolocation"));
            statement.setString(6, data.getJSONObject("data").getJSONObject("channel_details").getString("user_agent"));
            statement.setString(7, data.getJSONObject("data").getJSONObject("channel_details").getString("user_agent_version"));
            statement.setString(8, data.getJSONObject("data").getJSONObject("channel_details").getString("channel"));
            statement.setString(9, data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_type"));
            statement.setString(10, data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_code"));
            statement.setString(11, data.getJSONObject("data").getJSONObject("transaction_details").getString("host_code"));
            statement.setString(12, data.getJSONObject("data").getJSONObject("channel_details").getString("client_id"));
            statement.setString(13, data.getJSONObject("data").getJSONObject("transaction_details").getString("debit_account"));
            statement.setString(14, data.getJSONObject("data").getJSONObject("transaction_details").getString("phone_number"));
            statement.setString(15, String.valueOf(data.getLong("esbref")));
            int i = statement.executeUpdate();
            if (i > 0) {
                successful = true;
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        } finally {
            DB.close();
        }
        return successful;
    }
@Asynchronous
    public boolean updatemessage(JSONObject data) {
        boolean successful = false;
        DBConn DB = new DBConn();
        String SP_SQL = "UPDATE tbmessages SET biller_ref = ?, response_code = ?, response_message = ?,error_description = ?,charge_amount = ?,vat_amount = ?,commission_amount = ?,is_cbs = ? WHERE esb_reference = ? AND channel_reference = ?";

        try (Connection connection = DB.dbConn();
                PreparedStatement statement = connection.prepareStatement(SP_SQL)) {
            statement.setString(1, String.valueOf(data.getLong("esbref")));
            statement.setString(2, data.getJSONObject("data").getJSONObject("response").getString("response_code"));
            statement.setString(3, data.getJSONObject("data").getJSONObject("response").getString("response"));
            statement.setString(4, data.getJSONObject("data").getJSONObject("response").has("error_data") ? data.getJSONObject("data").getJSONObject("response").getString("error_data") : "Successful");
            statement.setDouble(5, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("charge_amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("charge_amount") : 0));
            statement.setDouble(6, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("excise_duty_amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("excise_duty_amount") : 0));
            statement.setDouble(7, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("bank_commission_amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("bank_commission_amount") : 0));
            statement.setInt(8, (int) (data.getJSONObject("data").getJSONObject("transaction_details").has("is_cbs") ? data.getJSONObject("data").getJSONObject("transaction_details").getInt("is_cbs") : 0));
            statement.setString(9, String.valueOf(data.getLong("esbref")));
            statement.setString(10, data.getString("xref"));

            int i = statement.executeUpdate();
            if (i > 0) {
                successful = true;
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        } finally {
            DB.close();
        }
        return successful;
    }
@Asynchronous
    public JSONObject getcharges(JSONObject data) throws JSONException {
        DBConn DB = new DBConn();
        String ResponseCode = "";
        String SP_SQL = "{ call SP_GETCHARGES(?,?,?,?,?,?)}";
        try (Connection connection = DB.dbConn();
                CallableStatement proc = connection.prepareCall(SP_SQL)) {

            proc.setString(1, data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_type"));//V_FIELD3    
            proc.setInt(2, data.getJSONObject("data").getJSONObject("transaction_details").getInt("amount"));//V_FIELD4
            proc.setString(3, data.getJSONObject("data").getJSONObject("transaction_details").getString("host_code"));//V_FIELD24
            proc.setString(4, data.getJSONObject("data").getJSONObject("channel_details").getString("channel"));//V_FIELD32
            proc.setString(5, data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_code"));//V_FIELD100
            proc.registerOutParameter(6, OracleTypes.CURSOR);
            proc.executeUpdate();
            try (ResultSet rs = (ResultSet) proc.getObject(6);) {
                while (rs.next()) {
                    String response = rs.getString(1);

                    String[] Responsearray = response.split("\\|");
                    ResponseCode = Responsearray[0];

                    if (ResponseCode.equals("000")) {
                        data.getJSONObject("data").getJSONObject("transaction_details").put("commission_type", Responsearray[1]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("umoja_charge_amount", Responsearray[2]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("dc_charge_amount", Responsearray[3]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_amount", Responsearray[4]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("bank_commission_amount", Responsearray[5]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("excise_duty_amount", Responsearray[6]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("expense_charge_amount", Responsearray[7]);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_response", "00");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_responsemessage", "Successful");
                    } else {
                        data.getJSONObject("data").getJSONObject("transaction_details").put("commission_type", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("umoja_charge_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("dc_charge_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("bank_commission_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("excise_duty_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("expense_charge_amount", "0");
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_response", ResponseCode);
                        data.getJSONObject("data").getJSONObject("transaction_details").put("charge_responsemessage", "No charge has been set");
                    }

                }
            }

        } catch (Exception ex) {
            data.getJSONObject("data").getJSONObject("transaction_details").put("commission_type", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("umoja_charge_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("dc_charge_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("charge_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("bank_commission_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("excise_duty_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("expense_charge_amount", "0");
            data.getJSONObject("data").getJSONObject("transaction_details").put("charge_response", ResponseCode);
            data.getJSONObject("data").getJSONObject("transaction_details").put("charge_responsemessage", "Exception while getting charge");
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        } finally {
            DB.close();
        }

        return data;
    }
@Asynchronous
    public JSONObject getesbref(JSONObject data) throws JSONException {
        DBConn DB = new DBConn();
        String SP_SQL = "select ESB_SEQ.NEXTVAL from dual";
        try (Connection connection = DB.dbConn();
                PreparedStatement statement = connection.prepareStatement(SP_SQL)) {
            synchronized (this) {
                try (ResultSet rs = statement.executeQuery();) {
                    if (rs.next()) {
                        long rrn = rs.getLong(1);
                        data.put("esbref", rrn);
                    }
                }
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            data.put("esbref", "0");
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        } finally {
            DB.close();
        }

        return data;

    }

    /**
     *
     * @param data
     * @return
     * @throws JSONException
     */
    @Asynchronous
    public JSONObject postwallettransaction(JSONObject data) throws JSONException {
        DBConn DB = new DBConn();
        String SP_SQL = "{ call SP_WALLET_TRANSACTIONS(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)}";
        JSONObject responseOBJ = new JSONObject();

        try (Connection connection = DB.dbConn();
                CallableStatement statement = connection.prepareCall(SP_SQL)) {
            statement.setString(1, data.getJSONObject("data").getJSONObject("transaction_details").has("direction") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("direction") : "");
            statement.setString(2, data.getJSONObject("data").getJSONObject("transaction_details").has("phone_number") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("phone_number") : "");
            statement.setString(3, data.getJSONObject("data").getJSONObject("transaction_details").has("transaction_type") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_type") : "");
            statement.setDouble(4, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("amount") : 0));
            statement.setString(5, new SimpleDateFormat("hhmmss").format(new java.util.Date()));
            statement.setString(6, data.getJSONObject("data").getJSONObject("transaction_details").has("host_code") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("host_code") : "");
            statement.setDouble(7, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("charge_amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("charge_amount") : 0));
            statement.setDouble(8, (double) (data.getJSONObject("data").getJSONObject("transaction_details").has("excise_duty_amount") ? data.getJSONObject("data").getJSONObject("transaction_details").getDouble("excise_duty_amount") : 0));
            statement.setString(9, data.getJSONObject("data").getJSONObject("channel_details").has("channel") ? (String) data.getJSONObject("data").getJSONObject("channel_details").getString("channel") : "");
            statement.setString(10, data.has("esbref") ? String.valueOf(data.getLong("esbref")) : "");
            statement.setString(11, data.getJSONObject("data").getJSONObject("transaction_details").has("extra_data") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("extra_data") : "");
            statement.setString(12, data.getJSONObject("data").getJSONObject("transaction_details").has("sender_number") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("sender_number") : "");
            statement.setString(13, data.getJSONObject("data").getJSONObject("transaction_details").has("narration") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("narration") : "");
            statement.setString(14, data.getJSONObject("data").getJSONObject("transaction_details").has("token") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("token") : "");
            statement.setString(15, data.getJSONObject("data").getJSONObject("channel_details").has("client_id") ? (String) data.getJSONObject("data").getJSONObject("channel_details").getString("client_id") : "");
            statement.setString(16, data.getJSONObject("data").getJSONObject("transaction_details").has("transaction_code") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_code") : "");
            statement.setString(17, data.getJSONObject("data").getJSONObject("channel_details").has("client_id") ? (String) data.getJSONObject("data").getJSONObject("channel_details").getString("client_id") : "");
            statement.setString(18, data.getJSONObject("data").getJSONObject("transaction_details").has("debit_account") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("debit_account") : "");
            statement.setString(19, data.getJSONObject("data").getJSONObject("transaction_details").has("credit_account") ? (String) data.getJSONObject("data").getJSONObject("transaction_details").getString("credit_account") : "");
            statement.registerOutParameter(20, OracleTypes.CURSOR);
            statement.executeUpdate();
            synchronized (this) {
                try (ResultSet rs = (ResultSet) statement.getObject(20);) {
                    while (rs.next()) {
                        String strrespone = rs.getString(1);
                        System.out.println("Wallet Respose:" + strrespone);

                        String[] splitresponse = strrespone.split("\\|");
                        if (splitresponse.length > 0) {
                            responseOBJ.put("response_code", splitresponse[2]);
                            responseOBJ.put("response", splitresponse[3]);
                            responseOBJ.put("error_data", splitresponse[3]);
                            data.getJSONObject("data").put("response", responseOBJ);

                        }

                    }
                }
            }

        } catch (Exception ex) {
            responseOBJ.put("response_code", "57");
            responseOBJ.put("response", "Post to wallet failed");
            responseOBJ.put("error_data", "Invalid msg format");
            data.getJSONObject("data").put("response", responseOBJ);
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        }
        return data;
    }
}
