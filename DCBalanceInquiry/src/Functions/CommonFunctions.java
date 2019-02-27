/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Functions;

import Messaging.LogWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.ejb.Asynchronous;
import org.json.JSONException;
import org.json.JSONObject;


/**
 *
 * @author manmaish
 */
@Asynchronous
public class CommonFunctions {
    String currentKey = "";



    /**
     *
     * @param data
     * @return
     * @throws JSONException
     */
    @Asynchronous
    public boolean validatejson(JSONObject data) throws JSONException {
        boolean successful = false;
        try {
            if (data.getJSONObject("data").getJSONObject("transaction_details").has("direction")
                    && data.has("txntimestamp")
                    && data.has("xref")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("host")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("geolocation")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("user_agent")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("user_agent_version")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("channel")
                    && data.getJSONObject("data").getJSONObject("transaction_details").has("transaction_type")
                    && data.getJSONObject("data").getJSONObject("transaction_details").has("transaction_code")
                    && data.getJSONObject("data").getJSONObject("transaction_details").has("host_code")
                    && data.getJSONObject("data").getJSONObject("channel_details").has("client_id")
                    && data.getJSONObject("data").getJSONObject("transaction_details").has("debit_account")
                    && data.getJSONObject("data").getJSONObject("transaction_details").has("phone_number")) {
                successful = true;
            }

        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        }
        return successful;
    }
}
