package org.apache.rocketmq.example.transaction;

import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import com.alibaba.fastjson.JSON;

/**   
 * <p> 事务消息消费测试 </p>
 *   
 * @author: 三帝（sandi@maihaoche.com） 
 * @date: 2017年12月18日 下午2:17:15   
 * @since V1.0 
 */
public class RocketMQTransactionConsumer {
	private final String ROCKETMQ_GROUP = "sandy-demo";
	private final String ROCKETMQ_TOPIC = "TransactionTest";
	private final String ROCKETMQ_NAMESRV = "127.0.0.1:9876";

	/**
	 * 接收消息
	 */
	public void receiveTransactionMessage() {
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(ROCKETMQ_GROUP);
		consumer.setNamesrvAddr(ROCKETMQ_NAMESRV);
		consumer.setVipChannelEnabled(false);
//		consumer.setConsumeThreadMax(20);
//		consumer.setConsumeThreadMin(10);
		consumer.setConsumeThreadMax(1);
		consumer.setConsumeThreadMin(1);
		try {
			consumer.subscribe(ROCKETMQ_TOPIC, "transaction");

			consumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(new ConsumeMessageHook() {
				@Override
				public String hookName() {
					return "ConsumeMessageHook";
				}

				@Override
				public void consumeMessageBefore(ConsumeMessageContext context) {
					System.out.println(JSON.toJSONString(context, true));
				}

				@Override
				public void consumeMessageAfter(ConsumeMessageContext context) {
					System.out.println(JSON.toJSONString(context, true));
				}
			});

			// 程序第一次启动从消息队列头取数据
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(new MessageListenerConcurrently() {
				public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list,
						ConsumeConcurrentlyContext Context) {
					Message msg = list.get(0);
					System.out.println(msg.toString());
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			consumer.start();
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		new RocketMQTransactionConsumer().receiveTransactionMessage();
	}
}
