package abd;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Random;

public class Workload {

  private static int MAX = (int) Math.pow(2, 15);
  private static int STRING_SIZE = 8100; // Quase 8k

  private static String getString(int length) {
    String alphanumeric = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    StringBuilder sb = new StringBuilder();
    Random random = new Random();

    for (int i = 0; i < length; i++) {
      int index = random.nextInt(alphanumeric.length());
      char randomChar = alphanumeric.charAt(index);
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

    s.executeUpdate("create table client (id int, address varchar, data varchar);");
    s.executeUpdate("create table product (id int, description varchar, data varchar);");
    s.executeUpdate("create table invoice (id int, productId int, clientId int, data varchar);");

    for (int i = 0; i < MAX; i++) {
      int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);
      String address = getString(30);
      String data = getString(STRING_SIZE);
      s.executeUpdate("insert into client values('" + clientId + "', '" + address + "', '" + data + "');");
    }

    for (int i = 0; i < MAX; i++) {
      int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
      String description = getString(30);
      String data = getString(STRING_SIZE);
      s.executeUpdate("insert into product values('" + productId + "', '" + description + "', '" + data + "');");
    }

    for (int i = 0; i < MAX; i++) {
      int invoiceId = rand.nextInt(Integer.MAX_VALUE) | rand.nextInt(Integer.MAX_VALUE);
      int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
      int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);
      String data = getString(STRING_SIZE);
      s.executeUpdate("insert into invoice values ('" + invoiceId + "', '" + productId + "', '" + clientId + "', '" + data + "');");
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
        s.executeUpdate("insert into invoice values ('" + invoiceId + "', '" + productId + "', '" + clientId + "');");
        break;
      case 1:
        s.executeQuery(
            "select distinct p.description from product p inner join invoice v on p.id = v.productid where v.clientid = "
                + clientId + ";");
        break;
      case 2:
        s.executeQuery(
            "select v.productid,count(v.productid) from invoice v group by v.productid order by count(v.productid) desc limit 10;");
        break;
      default:
        break;
    }

    s.close();
  }
}
