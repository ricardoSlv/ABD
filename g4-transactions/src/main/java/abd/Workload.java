package abd;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

public class Workload {

    private static int MAX = (int) Math.pow(2, 9);
    private static int STRING_SIZE = 8100; // Quase 8k

    private static String getString(int length) {
        String alphanumeric = "1234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

        StringBuilder sb = new StringBuilder();
        Random random = new Random();

        // TODO: Bulk insert and prepared statements
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

        s.executeUpdate("drop materialized view if exists top10view;");
        s.executeUpdate("drop table if exists client;");
        s.executeUpdate("drop table if exists product;");
        s.executeUpdate("drop table if exists invoice;");
        s.executeUpdate("drop table if exists invoiceLine;");
        s.executeUpdate("drop table if exists orders;");

        s.executeUpdate("create table client (id int, address varchar, data varchar);");
        s.executeUpdate(
                "create table product (id int, description varchar, stock int, min int, max int, data varchar);");
        s.executeUpdate("create table invoice (id int, productId int, clientId int, data varchar);");
        s.executeUpdate("create table invoiceLine (id int, invoiceId int, productId int, data varchar);");
        s.executeUpdate("create table orders (id int, productId int, supplier varchar, items int);");

        s.executeUpdate("create index c1 on client(id);");
        s.executeUpdate("create index p1 on product(id);");
        s.executeUpdate("create index v1 on invoice(id,productid,clientid);");
        s.executeUpdate("create index ivl1 on invoiceline(id,invoiceId,productId);");

        for (int i = 0; i < MAX; i++) {

            String address = getString(30);
            String description = getString(30);
            String data = getString(STRING_SIZE);

            int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
            int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);
            int invoiceId = rand.nextInt(MAX) | rand.nextInt(MAX);

            s.executeUpdate(String.format("insert into product values ('%d', '%s', '%d', '%d', '%d', '%s');", i,
                    description, 35, 20, 65, data));

            s.executeUpdate("insert into client values('" + i + "', '" + address + "', '" + data + "');");

            s.executeUpdate(String.format("insert into invoice values ('%d', '%d', '%d', '%s');", i, productId,
                    clientId, data));

            s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');", i, invoiceId,
                    productId, data));
            s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');", i, invoiceId,
                    productId, data));
            s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');", i, invoiceId,
                    productId, data));

        }

        s.close();
    }

    private final Random rand;
    private final Connection c;

    public Workload(Random rand, Connection c) throws Exception {
        this.rand = rand;
        this.c = c;

        c.setAutoCommit(false);

        // ---- DEMO WORKLOAD ----
        // initialize connection, e.g. c.setAutoCommit(false);
        // or create prepared statements...
        // -----------------------
    }

    public void transaction() throws Exception {
        Statement s = c.createStatement();
        int type = rand.nextInt(3);

        String data = getString(STRING_SIZE);

        int invoiceId = rand.nextInt(MAX) | rand.nextInt(MAX);
        int invoiceLineId = rand.nextInt(MAX) | rand.nextInt(MAX);
        int productId = rand.nextInt(MAX) | rand.nextInt(MAX);
        int clientId = rand.nextInt(MAX) | rand.nextInt(MAX);
        int orderId = rand.nextInt(MAX) | rand.nextInt(MAX);
        String supplier = rand.nextInt(1) > 0 ? "sup1" : "sup2";

        Boolean delivering = rand.nextInt(20) == 0;
        Boolean ordering = rand.nextInt(20) == 0;

        if (ordering) {

            // TODO Fix multiple orders for same present error still being possible
            // After inserting, see if there is more than 1 order for the product, and
            // rollback if so;

            // So that i dont commit if orders have been changed during my execution
            c.commit();
            c.setTransactionIsolation(8);
            ResultSet result = s.executeQuery("select p.id, p.stock, p.max from product p where p.stock<p.min "+
            "and (select coalesce(sum(items),0) from orders where productId = p.id) + p.stock < p.max");

            if (result.next()) {
                    
                    int prodId = result.getInt("id");
                    int stock = result.getInt("stock");
                    int max = result.getInt("max");
                    
                    ResultSet totOrdsRes = s.executeQuery(String.format("select sum(items) as tot from orders where productId = %d", prodId));
                    totOrdsRes.next();
                    int totOrds = totOrdsRes.getInt("tot");

                //ResultSet result2 = s.executeQuery(String.format("select count(*) as totalOrders from orders ord where ord.productId=%d", productId));
                //result2.next();
                //int ordersForProd = result2.getInt("totalOrders");
                //System.out.println("Curr orders: " + ordersForProd);

                //if (ordersForProd == 0) {
                //System.out.println("There were "+totOrds+" orders for "+ prodId);
                s.executeUpdate(String.format("insert into orders values (%d, %d, '%s', %d)", orderId,prodId, supplier, max - totOrds - stock));
                    //ResultSet result3 = s.executeQuery(String.format("select count(*) as totalOrders from orders ord where ord.productId=%d", productId));
                    //result3.next();

                    //int ordersForProdAfterOrder = result3.getInt("totalOrders");
                    //System.out.println("Orders after query: " + ordersForProdAfterOrder);
                    //if (ordersForProdAfterOrder > 1)
                    //    c.rollback();
                    //else
                    //    System.out.println("Ordered: " + prodId + " Status: " + status);
                //} else
                //    System.out.println("There was an order for: " + prodId);
                c.commit();
                c.setTransactionIsolation(2);

            }
        }

        if (delivering) {
                
            // So that i dont commit if orders have been changed during my execution
            c.commit();
            c.setTransactionIsolation(8);
                
            ResultSet result = s.executeQuery(
                    String.format("select id, productId, items from orders ord where ord.supplier='%s'", supplier));
            while (result.next()) {
                int orderIdRes = result.getInt("id");
                int prodId = result.getInt("productId");
                int itemsOrdered = result.getInt("items");
                Statement s2 = c.createStatement();
                ResultSet result2 = s2.executeQuery(String.format("select stock from product where id=%d", prodId));
                result2.next();
                int currStock = result2.getInt("stock");
                s2.executeUpdate(String.format("update product set stock=%d where id=%d", currStock + itemsOrdered, prodId));
                s2.executeUpdate(String.format("delete from orders where supplier='%s'", supplier));
                System.out.println(orderIdRes + " Supplied: " + prodId);
                s2.close();
            }
            c.commit();
            c.setTransactionIsolation(2);
        }

        switch (type) {
            case 0:
                // long i1=System.currentTimeMillis();
                ResultSet result = s
                        .executeQuery(String.format("select stock from product p where p.id=%d", productId));
                result.next();
                int currStock = result.getInt("stock");

                if (currStock >= 3) {
                    s.executeUpdate(String.format("update product set stock=%d where id=%d", currStock - 3, productId));
                    s.executeUpdate(String.format("insert into invoice values ('%d', '%d', '%d', '%s');", invoiceId,
                            productId, clientId, data));
                    s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');",
                            invoiceLineId, invoiceId, productId, data));
                    s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');",
                            invoiceLineId, invoiceId, productId, data));
                    s.executeUpdate(String.format("insert into invoiceLine values ('%d', '%d', '%d', '%s');",
                            invoiceLineId, invoiceId, productId, data));
                }
                // System.out.println(currStock);
                // long i2=System.currentTimeMillis();
                // System.out.println("0 -> " + (i2-i1));
                break;
            case 1:
                // List products bought by client
                // long i3=System.currentTimeMillis();
                s.executeQuery(" select distinct p.description" + " from product p, invoice v, invoiceline ivl"
                        + " where v.clientid = " + clientId + " and ivl.productId = p.id"
                        + " and ivl.invoiceId = v.id;");
                // long i4=System.currentTimeMillis();
                // System.out.println("1 -> " + (i4-i3));
                break;
            case 2:
                // long i5=System.currentTimeMillis();
                s.executeQuery(" select ivl.productid, count(ivl.productid)" + " from invoiceline ivl"
                        + " group by ivl.productid" + " order by count(ivl.productid) desc" + " limit 10;");
                // long i6=System.currentTimeMillis();
                // System.out.println("2 -> " + (i6-i5));
                break;
            default:
                break;
        }

        c.commit();
        s.close();
    }
}
