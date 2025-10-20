import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// 货物类
class Goods {
    private String name;  // 品名
    private double price; // 价格

    public Goods(String name, double price) {
        this.name = name;
        this.price = price;
    }

    @Override
    public String toString() {
        return "Goods{name='" + name + "', price=" + price + "}";
    }
}

// 生产者线程（存放货物）
class Producer implements Runnable {
    private BlockingQueue<Goods> queue;

    public Producer(BlockingQueue<Goods> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= 5; i++) { // 模拟存放5个货物
                Goods goods = new Goods("商品" + i, 10.0 * i);
                queue.put(goods); // 存放货物
                System.out.println("生产者存放: " + goods);
                Thread.sleep(500); // 模拟生产时间
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

// 消费者线程（取出货物）
class Consumer implements Runnable {
    private BlockingQueue<Goods> queue;

    public Consumer(BlockingQueue<Goods> queue) {
        this.queue = queue;
    }

    @Override
    public void run() {
        try {
            for (int i = 1; i <= 5; i++) { // 模拟取出5个货物
                Goods goods = queue.take(); // 取出货物
                System.out.println("消费者取出: " + goods);
                Thread.sleep(1000); // 模拟消费时间
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

public class GoodsStorage {
    public static void main(String[] args) {
        // 创建一个阻塞队列，容量为10
        BlockingQueue<Goods> queue = new LinkedBlockingQueue<>(10);

        // 创建生产者和消费者线程
        Thread producerThread = new Thread(new Producer(queue));
        Thread consumerThread = new Thread(new Consumer(queue));

        // 启动线程
        producerThread.start();
        consumerThread.start();
    }
}

