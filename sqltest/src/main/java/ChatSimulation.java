import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// 对话线程
class ChatThread implements Runnable {
    private String name; // 线程名称
    private BlockingQueue<String> incomingQueue; // 接收消息的队列
    private BlockingQueue<String> outgoingQueue; // 发送消息的队列

    public ChatThread(String name, BlockingQueue<String> incomingQueue, BlockingQueue<String> outgoingQueue) {
        this.name = name;
        this.incomingQueue = incomingQueue;
        this.outgoingQueue = outgoingQueue;
    }

    @Override
    public void run() {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            // 发送消息
            System.out.print(name + "，请输入消息（输入exit退出）：");
            String message = scanner.nextLine();
            if ("exit".equalsIgnoreCase(message)) {
                break;
            }
            try {
                outgoingQueue.put(name + "：" + message); // 将消息放入发送队列
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // 接收消息
            try {
                String receivedMessage = incomingQueue.take(); // 从接收队列中取出消息
                System.out.println(name + " 收到消息：" + receivedMessage);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        scanner.close();
    }
}

public class ChatSimulation {
    public static void main(String[] args) {
        // 创建两个阻塞队列，用于两个线程之间的消息传递
        BlockingQueue<String> queue1 = new LinkedBlockingQueue<>();
        BlockingQueue<String> queue2 = new LinkedBlockingQueue<>();

        // 创建两个对话线程
        Thread thread1 = new Thread(new ChatThread("线程A", queue1, queue2));
        Thread thread2 = new Thread(new ChatThread("线程B", queue2, queue1));

        // 启动线程
        thread1.start();
        thread2.start();
    }
}

