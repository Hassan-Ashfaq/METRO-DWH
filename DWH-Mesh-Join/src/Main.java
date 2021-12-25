import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.Month;
import java.util.*;

public class Main {
    public static <MultiHashMap, Int> void main(String[] args) {
        try{
            Scanner sc= new Scanner(System.in);
            System.out.print("Enter a DataBase Name: ");
            String db= sc.nextLine();
            System.out.print("Enter Your User Name: ");
            String user_name= sc.nextLine();
            System.out.print("Enter Your Password: ");
            String password= sc.nextLine();

            Connection connect_DB = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/"+db, user_name, password);
            Statement stmt = connect_DB.createStatement();

            Connection connect_DWH = DriverManager.getConnection(
                    "jdbc:mysql://127.0.0.1:3306/i191708_DWH", user_name, password);
            Statement DWH_stat = connect_DWH.createStatement();
            System.out.println("\n");

            int DB_transactions = 0;
            int DB_masterdata = 0;
            Map <String, List<List<String>>> MultiHashMap = new HashMap<String, List<List<String>>>();
            Queue<List> Transactions_Queue = new LinkedList<>();

            int batch_no = 1;
            for(int Outer=0; Outer<210; Outer++) {
                ResultSet DB_result = stmt.executeQuery("SELECT * FROM transactions limit 50 offset "+DB_transactions);
                List<List> Queue = new LinkedList<>();
                while(DB_result.next()) {
                    int columnsNumber = DB_result.getMetaData().getColumnCount();
                    List<String> all_data = new ArrayList<String>();
                    for (int i=1; i<=columnsNumber; i++) {
                        all_data.add(DB_result.getString(i));
                    }
                    if (MultiHashMap.containsKey(DB_result.getString(2))){
                        MultiHashMap.get(DB_result.getString(2)).add(all_data);
                    }else{
                        MultiHashMap.put(DB_result.getString(2), new ArrayList<List<String>>());
                        MultiHashMap.get(DB_result.getString(2)).add(all_data);
                    }
                    List<String> list=new ArrayList<String>();
                    list.add(DB_result.getString(1));
                    list.add(DB_result.getString(2));
                    Queue.add(list);
                }
                Transactions_Queue.add(Queue);

                ResultSet Master_data = stmt.executeQuery("SELECT * FROM masterdata limit 10 offset "+DB_masterdata);
                List<List> Master_data_List = new LinkedList<>(); // List to Store MasterData Data to Perform JOIN
                while(Master_data.next()) {
                    int columnsNumber = Master_data.getMetaData().getColumnCount();
                    List<String> all_master_data = new ArrayList<String>();
                    for (int i=1; i<=columnsNumber; i++) {
                        all_master_data.add(Master_data.getString(i));
                    }
                    Master_data_List.add(all_master_data);
                }

                for (List MD: Master_data_List) {
                    String MD_prod = (String) MD.get(0);
                    for (List item: Transactions_Queue) {
                        for(int x=0; x<item.size(); x++) {
                            List temp = (List<String>) item.get(x);
                            String Prod_id = (String) temp.get(1);
                            int tran_id = (int) Integer.parseInt((String) temp.get(0));
                            if (MD_prod.equals(Prod_id)){
                                for(int t=0; t<MultiHashMap.get(Prod_id).size(); t++) {
                                    int MD_prod_id = (int) Integer.parseInt((String) temp.get(0));
                                    if (tran_id==MD_prod_id){
                                        if(MultiHashMap.get(Prod_id).get(t).size()==8) {
                                            for (int data=1; data<(MD.size()-1); data++) {
                                                MultiHashMap.get(Prod_id).get(t).add((String) MD.get(data));
                                            }
                                            double Total_sale = Double.parseDouble(((String) MD.get(MD.size() - 1)));
                                            Total_sale *= Double.parseDouble(((String) MultiHashMap.get(Prod_id).get(t).get(7)));
                                            MultiHashMap.get(Prod_id).get(t).add(String.valueOf((Total_sale)));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                DB_transactions += 50;
                DB_masterdata += 10;
                if (Transactions_Queue.size()==10){
                    List Top = (List) ((LinkedList<List>) Transactions_Queue).peek();
                    List<List> Send_List = new LinkedList<>();
                    for(int fetch=0; fetch<Top.size(); fetch++){
                        List temp = (List<String>) Top.get(fetch);
                        String Prod_id = (String) temp.get(1);
                        String tran_id = (String) temp.get(0);
                        for(int t=0; t<MultiHashMap.get(Prod_id).size(); t++) {
                            if (tran_id.equals(MultiHashMap.get(Prod_id).get(t).get(0))){
                                Send_List.add(MultiHashMap.get(Prod_id).get(t));
                                MultiHashMap.get(Prod_id).remove(t);
                            }
                        }
                    }
                    for(int f=0; f<Send_List.size(); f++){
//                        System.out.println(Send_List.get(f));
                        try{
                            String Query_A ="Insert into STORE (STORE_ID, STORE_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Send_List.get(f).get(4));
                            ins.setString(2, (String) Send_List.get(f).get(5));
                            ins.executeUpdate();
                        }catch(Exception e){}
                        try{
                            String Query_A ="Insert into PRODUCT (PRODUCT_ID, PRODUCT_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Send_List.get(f).get(1));
                            ins.setString(2, (String) Send_List.get(f).get(8));
                            ins.executeUpdate();
                        }catch(Exception e){}
                        try{
                            String Query_A ="Insert into SUPPLIER (SUPPLIER_ID, SUPPLIER_NAME) values (?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Send_List.get(f).get(9));
                            ins.setString(2, (String) Send_List.get(f).get(10));
                            ins.executeUpdate();
                        }catch(Exception e){}
                        try{
                            LocalDate currentDate = LocalDate.parse((String) Send_List.get(f).get(6));
                            Month month = currentDate.getMonth();
                            DayOfWeek day = currentDate.getDayOfWeek();
                            String Query_A ="Insert into RECORD (DATE_ID, DAY_NAME, MONTH_NAME, QUARTER_NO, YEAR) " +
                                    "values (?, ?, ?, ?, ?)";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setDate(1, Date.valueOf((String) Send_List.get(f).get(6)));
                            ins.setString(2, String.valueOf(day));
                            ins.setString(3, String.valueOf(month));
                            String month_name =  String.valueOf(month);
                            if(month_name.equals("JANUARY") || month_name.equals("FEBRUARY") || month_name.equals("MARCH") ){
                                ins.setInt(4, 1);
                            }else if(month_name.equals("APRIL") || month_name.equals("MAY") || month_name.equals("JUNE") ){
                                ins.setInt(4, 2);
                            } else if(month_name.equals("JULY") || month_name.equals("AUGUST") || month_name.equals("SEPTEMBER") ){
                                ins.setInt(4, 3);
                            }else if(month_name.equals("OCTOBER") || month_name.equals("NOVEMBER") || month_name.equals("DECEMBER") ){
                                ins.setInt(4, 4);
                            }
                            ins.setInt(5, currentDate.getYear());
                            ins.executeUpdate();
                        }catch(Exception e){}
                        try{
                            String Query_A ="Insert into SALES (STORE_ID, SUPPLIER_ID, PRODUCT_ID, DATE_ID, TOTAL_QUANTITY, " +
                                    "TOTAL_SALES) values (?, ?, ?, ?, ?, ?); ";
                            PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                            ins.setString(1, (String) Send_List.get(f).get(4));
                            ins.setString(2, (String) Send_List.get(f).get(9));
                            ins.setString(3, (String) Send_List.get(f).get(1));
                            ins.setDate(4, Date.valueOf((String) Send_List.get(f).get(6)));
                            ins.setString(5, (String) Send_List.get(f).get(7));
                            ins.setString(6, (String) Send_List.get(f).get(11));
                            ins.executeUpdate();
                        }catch(Exception e){
                            try{
                                String Query_A ="UPDATE SALES SET TOTAL_QUANTITY=TOTAL_QUANTITY+"+Send_List.get(f).get(7)
                                        +" Where STORE_ID=? and SUPPLIER_ID=? and PRODUCT_ID=? and DATE_ID=?";
                                PreparedStatement ins = DWH_stat.getConnection().prepareStatement(Query_A);
                                ins.setString(1, (String) Send_List.get(f).get(4));
                                ins.setString(2, (String) Send_List.get(f).get(9));
                                ins.setString(3, (String) Send_List.get(f).get(1));
                                ins.setDate(4, Date.valueOf((String) Send_List.get(f).get(6)));
                                ins.executeUpdate();
                            }catch (Exception A){}
                            try{
                                String Query_B ="UPDATE SALES SET TOTAL_SALES=TOTAL_SALES+"+Send_List.get(f).get(11)
                                        +" Where STORE_ID=? and SUPPLIER_ID = ? and PRODUCT_ID = ? and DATE_ID = ?";
                                PreparedStatement ins_A = DWH_stat.getConnection().prepareStatement(Query_B);
                                ins_A.setString(1, (String) Send_List.get(f).get(4));
                                ins_A.setString(2, (String) Send_List.get(f).get(9));
                                ins_A.setString(3, (String) Send_List.get(f).get(1));
                                ins_A.setDate(4, Date.valueOf((String) Send_List.get(f).get(6)));
                                ins_A.executeUpdate();
                            }catch (Exception B){}
                        }
                    }
                    System.out.println("Batch No "+batch_no+" Send to DWH");
                    batch_no +=1;
                    ((LinkedList<List>) Transactions_Queue).pop();
                    if(DB_masterdata == 100){
                        DB_masterdata = 0;
                    }
                }
            }
            System.out.println("\nData Send To DataWareHouse Successfully !");
            connect_DWH.close();
            connect_DB.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}