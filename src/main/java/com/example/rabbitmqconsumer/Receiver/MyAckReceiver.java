package com.example.rabbitmqconsumer.Receiver;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class MyAckReceiver implements ChannelAwareMessageListener {

    @Override
    public void onMessage(Message message, Channel channel) throws Exception {
        long deliveTag = message.getMessageProperties().getDeliveryTag();
        try{
            String msg = message.toString();
            String[] msgArray = msg.split("'");
            Map<String,String> msgMap = mapStringToMap(msgArray[1].trim(),3);
            String messageId = msgMap.get("messageId");
            String messageData = msgMap.get("messageData");
            String createTime = msgMap.get("createTime");

            if ("TestDirectQueue".equals(message.getMessageProperties().getConsumerQueue())){
                System.out.println("消费的消息来自的队列名为："+message.getMessageProperties().getConsumerQueue());
                System.out.println("消息成功消费到  messageId:"+messageId+"  messageData:"+messageData+"  createTime:"+createTime);
                System.out.println("执行TestDirectQueue中的消息的业务处理流程......");

            }

            if ("fanout.A".equals(message.getMessageProperties().getConsumerQueue())){
                System.out.println("消费的消息来自的队列名为："+message.getMessageProperties().getConsumerQueue());
                System.out.println("消息成功消费到  messageId:"+messageId+"  messageData:"+messageData+"  createTime:"+createTime);
                System.out.println("执行fanout.A中的消息的业务处理流程......");

            }


            channel.basicAck(deliveTag,true);
        }catch (Exception e){
            channel.basicReject(deliveTag,false);
            e.printStackTrace();
        }
    }

//    {key=value,key=value,key=value}格式转换成map
    private Map<String,String> mapStringToMap(String str,int entryNum){
        str = str.substring(1,str.length() - 1);
        String[] strs = str.split(",",entryNum);
        Map<String,String> map = new HashMap<String,String>();
        for (String string : strs) {
            String key = string.split("=")[0].trim();
            String value = string.split("=")[1];
            map.put(key,value);
        }
        return map;
    }
}
