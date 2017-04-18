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
import java.util.List;

/**
 *
 * @author luc
 */
public class FileInfoCacheback1 {

    private static RedisCache instance() {
        return new RedisCache(Chiron.url, Chiron.psw, Chiron.port);
    }

    public static void cacheFile(EFile file, EActivation activation) {
        String fkey;
        if (file.getFileDir() != null) {
            String key = activation.activityId + "_" + activation.id;
            int fNo;
            RedisCache rc = instance();
            if (rc.existe(key)) {
                fNo = rc.getCacheNO(key) + 1;
            } else {
                fNo = 0;
            }
            if (file.getFileName() != null) {
                fkey = file.getFileDir() + "_" + file.getFileName();
            } else {
                fkey = file.getFileDir() + "_" + file.fieldName;
            }
            rc.cache(activation.activityId + "_" + activation.id + "_" + fNo, fkey);
            cacheFile(fkey, file);
            rc.cacheNO(key, fNo);
            rc.discon();
//            System.out.println("key: " + activation.activityId + "_" + activation.id + "_" + fNo + " fkey: " + fkey + " No: " + fNo);
//            Gson gson = new Gson();
//            System.out.println("File: " + gson.toJson(file) + " fieldName: " + file.fieldName);
        }
    }

    public static List<EFile> getFile(EActivation activation) {
        RedisCache rc = instance();
        String key = activation.activityId + "_" + activation.id;
//        System.out.println("key: " + key);
        List files = new ArrayList<>();
        if (!rc.existe(key)) {
            //            System.out.println("key: " + key + " has no files");
            return files;
        }
        for (int i = 0; i < rc.getCacheNO(key) + 1; i++) {
            EFile file = getFile(rc.getCache(key + "_" + i));
            if (file != null) {
                files.add(file);
            }
            //            System.out.println("key: " + rc.getCache(key + "_" + i) + "has file: " + file);
        }
        rc.discon();
        return files;
    }

    public static void cacheFile(EFile file) {
        String key = file.getFileDir() + "_" + file.getFileName();
        cacheFile(key, file);
    }

    public static int getFileSite(String fdir, String fname) {
        String key = fdir + "_" + fname;
        EFile file = getFile(key);
        if (file != null) {
            return file.getFsite();
        } else {
            return -1;
        }
    }

    public static void cacheIFile(EFile file) {
        int fNo;
        String key = file.getFileDir() + "_" + file.getFileName();
        RedisCache rc = instance();
        cacheFile(key, file);
        if (rc.existe(file.getFileDir())) {
            fNo = rc.getCacheNO(key) + 1;
        } else {
            fNo = 0;
        }
        rc.cache(file.getFileDir() + "_" + fNo, key);
        rc.cacheNO(file.getFileDir(), fNo);
        rc.discon();
    }

    public static List<EFile> getIOFiles(String fdir, int site) {
        RedisCache rc = instance();
        List files = new ArrayList<>();
        if (!rc.existe(fdir)) {
            return files;
        }
        for (int i = 0; i < rc.getCacheNO(fdir) + 1; i++) {
            EFile file = getFile(rc.getCache(fdir + "_" + i));
            if ((file != null) && (file.getFsite() != site)) {
                files.add(file);
            }
        }
        rc.discon();
        return files;
    }

    public static EFile getFile(EFile f) {
        String key = f.getFileDir() + "_" + f.getFileName();
        EFile file = getFile(key);
        if (file != null) {
            f.fileSize = file.fileSize;
            f.setFsite(file.getFsite());
        } else {
            f.setFsite(-1);
            f.fileSize = -1L;
        }
        return f;
    }

    private static void cacheFile(String key, EFile f) {
        RedisCache rc = instance();
        int fNo;
        if (rc.existe(key)) {
            fNo = existFile(key, f.getFsite());
            if (fNo == -1) {
                fNo = rc.getCacheNO(key) + 1;
            }
        } else {
            fNo = 0;
        }
        rc.cache(key + "_" + fNo, f);
        rc.cache(key, fNo);
        rc.discon();
    }

    private static int existFile(String key, int site) {
        RedisCache rc = instance();
        EFile file = null;
        Gson gson = new Gson();
//        System.out.println("[getFile] key: " + key );
        if (key.contains("\"")) {
            key = key.replace("\"", "");
//            System.out.println("[getFile] key: " + key + " replace.");
        }
        if (!rc.existe(key)) {
            //            System.out.println("[getFile] key: " + key + " has no files");
            return -1;
        }
        for (int i = 0; i < rc.getCacheNO(key) + 1; i++) {
            file = gson.fromJson(rc.getCache(key + "_" + i), EFile.class);
            if (file.getFsite() == site) {
                return i;
            }
        }
        rc.discon();
        return -1;
    }

    private static EFile getFile(String key) {
        RedisCache rc = instance();
        EFile result = null;
        EFile file = null;
        Gson gson = new Gson();
//        System.out.println("[getFile] key: " + key );
        if (key.contains("\"")) {
            key = key.replace("\"", "");
//            System.out.println("[getFile] key: " + key + " replace.");
        }
        if (!rc.existe(key)) {
            //            System.out.println("[getFile] key: " + key + " has no files");
            return null;
        }
        for (int i = 0; i < rc.getCacheNO(key) + 1; i++) {
            file = gson.fromJson(rc.getCache(key + "_" + i), EFile.class);
            if (file.getFsite() == Chiron.site && file.fieldName != null && file.fileSize != 0 && file.fileOper.equals(EFile.Operation.COPY)) {
                return file;
            } else if (file.fieldName != null && (result == null || result.fieldName == null)) {
                result = file;
            } else if (result == null) {
                result = file;
            } else if (result.fileSize == 0 && (file.fileSize != 0 && file.fieldName != null)) {
                result = file;
            } else if (file.getFsite() == Chiron.site && file.fieldName != null && file.fileSize != 0) {
                result = file;
            } else if ((result.getFsite() != Chiron.site) && file.fieldName != null && file.fileSize != 0 && file.fileOper.equals(EFile.Operation.COPY)) {
                result = file;
            }
            //            System.out.println("[getFile] key: " + key + "_" + i + "has file: " + file);
        }
        rc.discon();
        return result;
    }

}
