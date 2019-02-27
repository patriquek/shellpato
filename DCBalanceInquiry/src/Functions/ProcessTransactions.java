/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Functions;

import Database.DBFunctions;
import Messaging.LogWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.ejb.Asynchronous;
import javax.servlet.http.HttpServletRequest;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author manmaish
 */
@Asynchronous
public class ProcessTransactions {

    public ProcessTransactions() {
    }

    /**
     *
     * @param request
     * @param channelMessage
     * @return
     * @throws JSONException
     */
    @Asynchronous
    public JSONObject ProcessBIRequests(HttpServletRequest request, String channelMessage) throws JSONException {
        JSONObject responseOBJ = new JSONObject();
        DBFunctions dbf = new DBFunctions();
        CommonFunctions cm = new CommonFunctions();
        JSONObject jsonObj = new JSONObject(channelMessage);

        try {
            String ipAddress = request.getHeader("X-FORWARDED-FOR");
            if (ipAddress == null) {
                ipAddress = request.getRemoteAddr();
                jsonObj.getJSONObject("data").getJSONObject("channel_details").put("host", ipAddress);
            }

            //validate json
            if (!cm.validatejson(jsonObj)) {
                responseOBJ.put("response_code", "57");
                responseOBJ.put("response", "Invalid json format");
                responseOBJ.put("error_data", "Invalid json format");
                jsonObj.getJSONObject("data").put("response", responseOBJ);
                return jsonObj;
            }
            //lets get the esbref one unique ref 12digits to start with 1 coz of mpesa issues/so lets just sort it kabisa
            jsonObj = dbf.getesbref(jsonObj);
            long esbref = jsonObj.getLong("esbref");
            if (esbref == 0) {
                //means there is a datasource issue raise flag to critical queue
                responseOBJ.put("response_code", "57");
                responseOBJ.put("response", "Unable to generate esb ref");
                responseOBJ.put("error_data", "Unable to generate esb ref");
                jsonObj.getJSONObject("data").put("response", responseOBJ);
                return jsonObj;
            }
            //let's validate messages & check DB connection using insert then proceed 
            if (!dbf.insertmessage(jsonObj)) {
                //means there is a datasource issue raise flag to critical queue
                responseOBJ.put("response_code", "57");
                responseOBJ.put("response", "Save messages failed");
                responseOBJ.put("error_data", "Invalid msg format");
                jsonObj.getJSONObject("data").put("response", responseOBJ);
                return jsonObj;
            }
            //let's put a key here to check whether the service is enabled or disable
            //let's get charges
            jsonObj = dbf.getcharges(jsonObj);
            //let's get check if its a Wallet or Agency Transaction so that we understand routing of requests
            switch (jsonObj.getJSONObject("data").getJSONObject("transaction_details").getString("transaction_code")) {
                case "BIMM":
                    //case 1 Wallet:
                    ProcessWalletRequests pw = new ProcessWalletRequests();
                    jsonObj = pw.processwalletrequest(jsonObj);
                    break;

                case "BIMMA":
                    //case 2 Agency(Biometric):

                    break;

                default:
                    responseOBJ.put("response_code", "57");
                    responseOBJ.put("response", "Invalid Transaction Code");
                    responseOBJ.put("error_data", "Invalid Transaction Code");
                    jsonObj.getJSONObject("data").put("response", responseOBJ);
                    break;
            }
            //update response before responding
            dbf.updatemessage(jsonObj);

        } catch (JSONException ex) {
            responseOBJ.put("response_code", "57");
            responseOBJ.put("response", "channel authentication failed");
            responseOBJ.put("error_data", "Invalid msg format");
            jsonObj.getJSONObject("data").put("response", responseOBJ);
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            new LogWriter(sw.toString()).log("DCBI_ERROR");
        }
        return jsonObj;
    }

}
