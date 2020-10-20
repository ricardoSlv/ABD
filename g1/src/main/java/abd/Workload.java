package abd;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;

public class Workload {

  private static int MAX = (int) Math.pow(2, 10);

  private static String getString(int length) {
    String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    StringBuilder sb = new StringBuilder();
    Random random = new Random();

    for (int i = 0; i < length; i++) {
      int index = random.nextInt(alphabet.length());
      char randomChar = alphabet.charAt(index);
      sb.append(randomChar);
    }
    String randomString = sb.toString();
    return randomString;
  }

  public static void populate(Random rand, Connection c) throws Exception {
    Statement s = c.createStatement();

    s.executeUpdate("drop table if exists client");
    s.executeUpdate("drop table if exists product");
    s.executeUpdate("drop table if exists invoice");

    s.executeUpdate("create table Client (id int, address varchar, data varchar);");
    s.executeUpdate("create table Product (id int, description varchar, data varchar);");
    s.executeUpdate("create table Invoice (id int, productId int, clientId int);");


    for (int i = 0; i < MAX; i++) {
      int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);
      String address = getString(30);
      String data = getString(1200);
      s.executeUpdate("insert into client values('" + clientId + "', '" + address + "', '" + data + "');");
    }

    for (int i = 0; i < MAX; i++) {
      int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
      String description = getString(30);
      String data = getString(1200);
      s.executeUpdate("insert into product values('" + productId + "', '" + description + "', '" + data + "');");
    }

    s.close();
  }

  public static void transaction(Random rand, Connection c) throws Exception {
    Statement s = c.createStatement();
    int type = rand.nextInt(3);

    int invoiceId = rand.nextInt(Integer.MAX_VALUE) | rand.nextInt(Integer.MAX_VALUE);
    int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
    int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);

    switch (type) {
      case 0:
        s.executeUpdate("insert into invoice values ('"+invoiceId+"', '"+productId+"', '"+clientId+"');");
        break;
      case 1:
        s.executeQuery("select distinct p.description from product p inner join invoice v on p.id = v.productid where v.clientid = "+clientId+";");
        break;
      case 2:
        s.executeQuery("select v.productid,count(v.productid) from invoice v group by v.productid order by count(v.productid) desc limit 10;");
        break;
      default:
        break;
    }

    s.close();
  }
}
