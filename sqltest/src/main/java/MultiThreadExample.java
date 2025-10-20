public class MultiThreadExample {
    // 创建一个共享对象用于同步
    private static final Object lock = new Object();

    public static void main(String[] args) {
        // 创建线程A
        Thread threadA = new Thread(() -> {
            synchronized (lock) {
                System.out.println("Thread A: ...start");
                try {
                    lock.wait(); // 等待通知
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Thread A: ...end");
            }
        });

        // 创建线程B
        Thread threadB = new Thread(() -> {
            synchronized (lock) {
                System.out.println("Thread B: ...start");
                try {
                    lock.wait(); // 等待通知
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Thread B: ...end");
            }
        });

        // 创建线程C
        Thread threadC = new Thread(() -> {
            try {
                Thread.sleep(2000); // 休眠2秒
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            synchronized (lock) {
                lock.notifyAll(); // 唤醒所有等待的线程
            }

            // 等待线程A和线程B结束
            try {
                threadA.join();
                threadB.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Thread C: All threads have finished.");
        });

        // 启动线程
        threadA.start();
        threadB.start();
        threadC.start();
    }
}

