package org.apache.rocketmq.example.transaction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.PropertyKeyConst;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**   
 * <p> 事务消息生产端测试 </p>
 *   
 * @author: 三帝（sandi@maihaoche.com） 
 * @date: 2017年12月18日 下午2:17:34   
 * @since V1.0 
 */
public class TransactionProducer {

	public static Map<Integer, Boolean> statsMap = new ConcurrentHashMap<>();

	public static void main(String[] args) throws MQClientException, InterruptedException {
		TransactionCheckListener transactionCheckListener = new TransactionCheckListenerImpl();
		TransactionMQProducer producer = new TransactionMQProducer("sandy-demo");
		producer.setCheckThreadPoolMinSize(2);
		producer.setCheckThreadPoolMaxSize(2);
		producer.setCheckRequestHoldMax(2000);
		producer.setNamesrvAddr("127.0.0.1:9876");
		// producer.setNamesrvAddr("testserver004:9876");
		producer.setTransactionCheckListener(transactionCheckListener);
		producer.start();

		String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };
		TransactionExecuterImpl tranExecuter = new TransactionExecuterImpl();
		ExecutorService executorService = Executors.newFixedThreadPool(20);
		for (int j = 0; j < 5000; j++) {
			final int i = j;
			executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						int xx = i;
						Message msg = new Message("TransactionTest", tags[i % tags.length], "KEY" + i,
								String.valueOf(xx).getBytes(RemotingHelper.DEFAULT_CHARSET));
						long startTime = System.currentTimeMillis();
						msg.putUserProperty(PropertyKeyConst.CheckImmunityTimeInSeconds, "30");
						// producer.send(msg);
						SendResult sendResult = producer.sendMessageInTransaction(msg, tranExecuter, i);
						// System.out.println("发送消息:" + sendResult + ",消耗时间:" +
						// (System.currentTimeMillis() - startTime) + "ms");
						System.out.println(xx+"发送消息:" + sendResult + ",消耗时间:" + (System.currentTimeMillis() - startTime) + "ms");
						statsMap.put(i, true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			});
		}

		for (int i = 0; i < 100000; i++) {
			Thread.sleep(1000);
		}
		producer.shutdown();
	}
}
