/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Functions;

import Database.Configurations;
import Database.DBFunctions;
import Messaging.LogWriter;
import Messaging.QueueWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.ejb.Asynchronous;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author manmaish
 */
public class ProcessWalletRequests {

    /**
     *
     * @param walletrequestOBJ
     * @return
     * @throws JSONException
     */
    @Asynchronous
    public JSONObject processwalletrequest(JSONObject walletrequestOBJ) throws JSONException {
        JSONObject responseOBJ = new JSONObject();
        DBFunctions dbf = new DBFunctions();
        String[] mwalletAccountBalance;
        double availableBalance = 0.0;
        double actualBalance = 0.0;
        String accountbalance = "";

        try {
            //let's check if kama kuna charge before responding to customer
            double charge = walletrequestOBJ.getJSONObject("data").getJSONObject("transaction_details").getDouble("charge_amount");

            if (charge > 0) {
                // lets pass this to wallet
                walletrequestOBJ = dbf.postwallettransaction(walletrequestOBJ);
                if (walletrequestOBJ.getJSONObject("data").getJSONObject("response").getString("response_code").equalsIgnoreCase("00")) {
                    accountbalance = dbf.getaccountbalance(walletrequestOBJ.getJSONObject("data").getJSONObject("transaction_details").getString("debit_account"));
                    if (accountbalance == null || accountbalance.isEmpty() || !(accountbalance.length() > 0)) {
                        walletrequestOBJ.getJSONObject("data").getJSONObject("response").put("response_code", "57");
                        walletrequestOBJ.getJSONObject("data").getJSONObject("response").put("response", "Unable to get balance");
                        walletrequestOBJ.getJSONObject("data").getJSONObject("response").put("error_data", "Unable to get balance");
                        return walletrequestOBJ;
                    } else {
                        mwalletAccountBalance = accountbalance.replace("|", "<@@>").split("<@@>");
                        availableBalance = Float.parseFloat(mwalletAccountBalance[0]);
                        actualBalance = Float.parseFloat(mwalletAccountBalance[1]);
                        walletrequestOBJ.getJSONObject("data").getJSONObject("response").put("available_balance", availableBalance);
                        walletrequestOBJ.getJSONObject("data").getJSONObject("response").put("actual_balance", actualBalance);
                        //let's send this request to the Charges Adapter for Posting;
                        walletrequestOBJ.getJSONObject("data").getJSONObject("transaction_details").put("is_cbs", 1);
                        try {
                            new QueueWriter(new Configurations().getConfig("CBS_CHARGES_QUEUE"), walletrequestOBJ).sendObjectToQueue();
                        } catch (Exception ex) {
                            StringWriter sw = new StringWriter();
                            ex.printStackTrace(new PrintWriter(sw));
                            new LogWriter(sw.toString()).log("DCBI_ERROR");
                        }

                    }

                } else {
                }
            } else {
                //no charge let's fetch it instantly
                accountbalance = dbf.getaccountbalance(walletrequestOBJ.getJSONObject("data").getJSONObject("transaction_details").getString("debit_account"));
                if (accountbalance == null || accountbalance.isEmpty() || !(accountbalance.length() > 0)) {
                    responseOBJ.put("response_code", "57");
                    responseOBJ.put("response", "Unable to get balance");
                    responseOBJ.put("error_data", "Unable to get balance");
                    walletrequestOBJ.getJSONObject("data").put("response", responseOBJ);
                    return walletrequestOBJ;
                } else {
                    mwalletAccountBalance = accountbalance.replace("|", "<@@>").split("<@@>");
                    availableBalance = Float.parseFloat(mwalletAccountBalance[0]);
                    actualBalance = Float.parseFloat(mwalletAccountBalance[1]);
                    responseOBJ.put("response_code", "00");
                    responseOBJ.put("response", "success");
                    responseOBJ.put("available_balance", availableBalance);
                    responseOBJ.put("actual_balance", actualBalance);
                    walletrequestOBJ.getJSONObject("data").put("response", responseOBJ);
                }

            }
            return walletrequestOBJ;
        } catch (JSONException | NumberFormatException ex) {
            responseOBJ.put("response_code", "57");
            responseOBJ.put("response", "channel authentication failed");
            responseOBJ.put("error_data", "Invalid msg format");
            walletrequestOBJ.getJSONObject("data").put("response", responseOBJ);
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        }
        return walletrequestOBJ;
    }  
}
