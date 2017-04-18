/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cache;

import chiron.Chiron;
import chiron.EActivation;
import chiron.EFile;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author luc
 */
public class FileInfoCache {

    private static RedisCache rc = null;

    private static void instance() {
        if (rc == null) {
            rc = new RedisCache(Chiron.url, Chiron.psw, Chiron.port);
        }
    }

    public static void cacheFile(EFile file, EActivation activation) {
        String tkey;
        if (file.getFileDir() != null) {
            instance();
            String key = activation.activityId + "_" + activation.id;
            if (file.getFileName() != null) {
                tkey = key + "_" + file.getFileDir() + "_" + file.fieldName + "_" + file.getFsite();
                if (rc.existe(tkey)) {
                    rc.removeCache(tkey);
                }
                key = key + "_" + file.getFileDir() + "_" + file.getFileName() + "_" + file.getFsite();
                rc.cache(key, file);
            } else {
                key = key + "_" + file.getFileDir() + "_" + file.fieldName + "_" + file.getFsite();
                rc.cache(key, file);
            }
        }
    }

    public static List<EFile> getFile(EActivation activation) {
        instance();
        String keyPre = activation.activityId + "_" + activation.id;
        Set<String> keys = rc.getKeys(keyPre + "*");
        Set<String> fkeys = new HashSet<>();
        for (String key : keys) {
            fkeys.add(key.substring(0, key.lastIndexOf("_")));
        }
        List files = new ArrayList<>();
        for (String key : fkeys) {
            files.add(getFile(key));
        }
        return files;
    }

    public static void cacheFile(EFile file) {
        String key = file.getFileDir() + "_" + file.getFileName() + "_" + file.getFsite();
        instance();
        rc.cache(key, file);
    }

    public static int getFileSite(String fdir, String fname) {
        EFile file = getFile(fdir, fname);
        if (file != null) {
            return file.getFsite();
        } else {
            return -1;
        }
    }

    public static void cacheIFile(EFile file) {
        String key = file.getFileDir() + "_" + file.getFileName() + "_" + file.getFsite();
        instance();
        rc.cache(key, file);
    }

    public static List<EFile> getIOFiles(String fdir, int site) {
        EFile file;
        instance();
        Set<String> keys = rc.getKeys("*" + fdir + "*");
        Set<String> fkeys = new HashSet<>();
        for (String key : keys) {
            if (key.indexOf(fdir) == 0) {
                continue;
            }
            fkeys.add(key.substring(0, key.lastIndexOf("_")));
        }
        List files = new ArrayList<>();
        for (String key : fkeys) {
            file = getFile(key);
            if (file.getFsite() != site) {
                if (file.getFileName() != null) {
                    files.add(getFile(key));
                }
            }
        }
        return files;
    }

    public static EFile getFile(EFile f) {
        EFile file = getFile(f.getFileDir(), f.getFileName());
        if (file != null) {
            f.fileSize = file.fileSize;
            f.setFsite(file.getFsite());
        } else {
            f.setFsite(-1);
            f.fileSize = -1L;
        }
        return f;
    }

    public static EFile getFile(String dir, String filename) {
        String fkey = null;
        String key = dir + "_" + filename;
        instance();
        Set<String> keys = rc.getKeys("*" + key + "_*");
        System.out.println("[getFile debug]: " + key + " key no. : " + keys.size());
        for (String nkey : keys) {
            if (fkey == null) {
                fkey = nkey;
            } else if (fkey.indexOf(dir) == 0 && nkey.indexOf(dir) != 0) {
                fkey = nkey;
            } else if (fkey.indexOf(dir) != 0 && Integer.parseInt(nkey.substring(nkey.lastIndexOf("_") + 1)) == Chiron.site) {
                fkey = nkey;
                break;
            }
        }
        if (fkey != null) {
            System.out.println("fkey: " + fkey + " value: " + rc.getCache(fkey));
            return (new Gson()).fromJson(rc.getCache(fkey), EFile.class);
        } else {
            return null;
        }
    }

    public static EFile getFile(String key) {
        String fkey = null;
        instance();
        Set<String> keys = rc.getKeys(key + "_*");
        for (String nkey : keys) {
            if (nkey.indexOf(key) != 0 && nkey.charAt(nkey.indexOf(key) - 1) != '_') {
                continue;
            }
            if (fkey == null) {
                fkey = nkey;
            } else if (Integer.parseInt(fkey.substring(fkey.lastIndexOf("_") + 1)) != Chiron.site && Integer.parseInt(nkey.substring(nkey.lastIndexOf("_") + 1)) == Chiron.site) {
                fkey = nkey;
                break;
            }
        }
        return (new Gson()).fromJson(rc.getCache(fkey), EFile.class);
    }

}
