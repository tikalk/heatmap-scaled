package com.tikal.fullstack.heatmap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import redis.clients.jedis.Jedis;

public class RedisPopulator {
	public static void main(String args[])  {
		Jedis jedis = new Jedis("localhost");
        
        
        InputStream ins = null; // raw byte-stream
        Reader r = null; // cooked reader
        BufferedReader br = null; // buffered for readLine()
        try {
            String s;
            ins = RedisPopulator.class.getResourceAsStream("/US.txt");
            r = new InputStreamReader(ins, "UTF-8"); // leave charset out for default
            br = new BufferedReader(r);
            int i=0;
            while ((s = br.readLine()) != null) {
            	String[] parts = s.split("\t");
            	String address = parts[2]+","+parts[3]+","+parts[4]+","+parts[5]+","+parts[6]+","+parts[7]+","+parts[8];
            	String longlat = parts[9]+","+parts[10];
            	jedis.set("ADDSEQ-"+i, "ADDRESS:"+address);
            	jedis.set("ADDRESS:"+address, longlat);
            	i++;
            }
        }
        catch (Exception e)
        {
            System.err.println(e.getMessage()); // handle exception
        }
        finally {
            if (br != null) { try { br.close(); } catch(Throwable t) { /* ensure close happens */ } }
            if (r != null) { try { r.close(); } catch(Throwable t) { /* ensure close happens */ } }
            if (ins != null) { try { ins.close(); } catch(Throwable t) { /* ensure close happens */ } }
        }
	}
}
