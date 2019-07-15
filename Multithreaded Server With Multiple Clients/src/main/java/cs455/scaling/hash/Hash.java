package cs455.scaling.hash;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

    private static Hash hasher = null;

    private Hash(){}

    public static Hash getInstance()
    {
        if (hasher == null)
            hasher = new Hash();
        return hasher;
    }

    public String toHash(byte[] data){
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA1");
        } catch (NoSuchAlgorithmException n){
            n.printStackTrace();
            System.out.println("Error, couldn't convert to SHA1!");
            return "ERROR from server";
        }
        byte[] hash = digest.digest(data);
        BigInteger hashInt = new BigInteger(1, hash);
        String almostHash = hashInt.toString(16);
        String zeroBuffer = "";
        if(almostHash.length() < 40){
            for(int i = almostHash.length(); i < 40; i++){
                zeroBuffer += "0";
            }
            almostHash = zeroBuffer + almostHash;
        }
        return almostHash;
    }



}
