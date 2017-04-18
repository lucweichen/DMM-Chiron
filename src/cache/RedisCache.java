/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cache;

import com.google.gson.Gson;
import java.util.Set;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

/**
 *
 * @author luc
 */
public class RedisCache {
    
    private final Jedis jedis;
    
    public RedisCache(String url, String psw, String port){
        JedisShardInfo shardInfo = new JedisShardInfo(url, port);
//        shardInfo.setPassword(psw); /* Use your access key. */
        jedis = new Jedis(shardInfo);
    }
    
    public void cache(String key, Object o){
        Gson gson = new Gson();
        String value = gson.toJson(o);
        synchronized(jedis){
            jedis.set(key, value);
        }
    }
    
    public String getCache(String key){
        synchronized(jedis){
            return jedis.get(key);
        }
    }
    
    public void removeCache(String[] keys){
        synchronized(jedis){
            jedis.del(keys);
        }
    }
    
    public void removeCache(String key){
        String[] keys = new String[1];
        keys[0] = key;
        synchronized(jedis){
            jedis.del(keys);
        }
    }
    
    public void cacheNO(String key, int value){
        Gson gson = new Gson();
        String cValue = gson.toJson(value);
        synchronized(jedis){
            jedis.set(key, cValue);
        }
    }
    
    public int getCacheNO(String key){
        synchronized(jedis){
            return Integer.parseInt(jedis.get(key));
        }
    }
    
    public boolean existe(String key){
        synchronized(jedis){
            return jedis.exists(key);
        }
    }
    
    public Set<String> getKeys(String pattern){
        synchronized(jedis){
            return jedis.keys(pattern);
        }
    }
    
    public void discon(){
        synchronized(jedis){
            jedis.disconnect();
        }
    }
    
}