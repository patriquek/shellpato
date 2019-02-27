/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Database;

import java.io.PrintWriter;
import java.io.StringWriter;
import javax.ejb.Asynchronous;
import redis.clients.jedis.Jedis;

/**
 *
 * @author manmaish
 */
@Asynchronous
public class Configurations {

    Jedis jedis = null;
@Asynchronous
    public void getRedisConnection() {
        try {
            this.jedis = new Jedis("192.168.20.104", 6379);
            this.jedis.auth("the@0fwar");
            this.jedis.select(0);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            System.err.println(sw.toString());
        }
    }
@Asynchronous
    public String getConfig(String keyString) {
        if (this.jedis == null) {
            getRedisConnection();
        } else if (!this.jedis.isConnected()) {
            getRedisConnection();
        }
        String valString = null;
        try {
            valString = this.jedis.get(keyString);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            System.err.println(sw.toString());
        } finally {
            if (this.jedis.isConnected()) {
                this.jedis.close();
            }
        }
        return valString;
    }
@Asynchronous
    public String setConfig(String objKey, String value) {
        String valString = null;
        try {
            if (this.jedis == null) {
                getRedisConnection();
            } else if (!this.jedis.isConnected()) {
                getRedisConnection();
            }
            valString = this.jedis.set(objKey, value);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            System.err.println(sw.toString());
        } finally {
            if (this.jedis.isConnected()) {
                this.jedis.close();
            }
        }
        return valString;
    }
@Asynchronous
    public Boolean setConfigObject(String key, String field, String value) {
        Long valString = -1L;
        try {
            if (this.jedis == null) {
                getRedisConnection();
            } else if (!this.jedis.isConnected()) {
                getRedisConnection();
            }
            valString = this.jedis.hset(key, field, value);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            System.err.println(sw.toString());
        } finally {
            if (this.jedis.isConnected()) {
                this.jedis.close();
            }
        }
        return valString > -1;
    }
@Asynchronous
    public String getConfigFromObject(String objKey, String keyString) {
        String valString = null;
        try {
            if (this.jedis == null) {
                getRedisConnection();
            } else if (!this.jedis.isConnected()) {
                getRedisConnection();
            }
            valString = this.jedis.hget(objKey, keyString);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            System.err.println(sw.toString());
        } finally {
            if (this.jedis.isConnected()) {
                this.jedis.close();
            }
        }
        return valString;
    }
}
