import java.io.IOException;
import java.util.Random;
import java.io.FileWriter;
import java.io.BufferedWriter;

public class CreateDataset {
    public static String generateName(){
        Random rand = new Random();
        String fname = "", lname = "";
        int fnamelen = rand.nextInt(5)+3;
        fname += (char)(rand.nextInt(26)+65);
        for (int i = 1; i<=fnamelen; i++){
            fname += (char)(rand.nextInt(26)+97);
        }
        int lnamelen = rand.nextInt(10-fname.length() - 1) + 10;
        lname += (char)(rand.nextInt(26)+65);
        for (int j = 1 ;j <= lnamelen; j++){
            lname += (char)(rand.nextInt(26)+97);
        }
        return fname + " " + lname;
    }

    public static String generateCust(int i){
        String[] g = {"Male","Female"};
        Random rand = new Random();
        int countryCode = rand.nextInt(10)+1, age = (int) (Math.random()*(70-9)) + 10;
        String name = generateName();
        double salary = Math.random()*(10000-99)+100;
        String tmp = Integer.toString(i)+"," + name;
        tmp += "," + Integer.toString(age) + "," + g[(rand.nextInt(2))] + "," + Integer.toString(countryCode) + "," +String.format("%.2f",salary);
        return tmp;
    }
    public static String generateTrans(int i){
        String alphabet = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ @!_`~$%^&*#-+=[]{}\\|?.;";
        Random rand = new Random();
        int CustID = rand.nextInt(49999)+1, TransNumItems = rand.nextInt(10)+1, strlen = rand.nextInt(30) + 20;
        double TransTotal = Math.random()*(1000-10)+10;

        String tmp = Integer.toString(i) + "," + Integer.toString(CustID) + "," + String.format("%.2f",TransTotal) + ","  + Integer.toString(TransNumItems) + ",";
        for(int j = 0; j<strlen;j++){
            tmp += alphabet.charAt(rand.nextInt(alphabet.length()));
        }
        return tmp;
    }
    public static void saveToFile(String file, int range) {
        try {
            FileWriter fw = new FileWriter(file);
            BufferedWriter bufw = new BufferedWriter(fw);
            for(int i = 1; i <= range; i++) {
                if (range == 50000){
                    bufw.write(generateCust(i));
                }
                else{
                    bufw.write(generateTrans(i));
                }
                bufw.newLine();
            }
            bufw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args){
        saveToFile("C:/Users/huawe/Desktop/DS503Project/Project1/data/Customer.txt", 50000);
        saveToFile("C:/Users/huawe/Desktop/DS503Project/Project1/data/Transaction.txt", 5000000);
    }
}