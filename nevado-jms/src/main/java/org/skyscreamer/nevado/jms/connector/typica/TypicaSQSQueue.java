package org.skyscreamer.nevado.jms.connector.typica;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.jms.JMSException;

import org.skyscreamer.nevado.jms.connector.SQSQueue;

import com.xerox.amazonws.sqs2.Message;
import com.xerox.amazonws.sqs2.MessageQueue;
import com.xerox.amazonws.sqs2.QueueAttribute;
import com.xerox.amazonws.sqs2.SQSException;

/**
 * Representation of a Typica queue
 * 
 * @author Carter Page <carter@skyscreamer.org>
 */
@Deprecated
class TypicaSQSQueue implements SQSQueue {
  private final TypicaSQSConnector _typicaSQSConnector;
  private final MessageQueue _sqsQueue;

  public TypicaSQSQueue(TypicaSQSConnector typicaSQSConnector, MessageQueue sqsQueue) {
    _typicaSQSConnector = typicaSQSConnector;
    _sqsQueue = sqsQueue;
  }

  @Override
  public void deleteQueue() throws JMSException {
    try {
      _sqsQueue.deleteQueue();
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to delete message queue '" + _sqsQueue.getUrl(), e);
    }
  }

  @Override
  public String sendMessage(String serializedMessage) throws JMSException {
    try {
      return _sqsQueue.sendMessage(serializedMessage);
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to send message to queue " + _sqsQueue.getUrl(), e);
    }
  }

  @Override
  public void setMessageVisibilityTimeout(String sqsReceiptHandle, int timeout) throws JMSException {
    try {
      _sqsQueue.setMessageVisibilityTimeout(sqsReceiptHandle, timeout);
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to reset message visibility for message "
          + "with receipt handle " + sqsReceiptHandle, e);
    }
  }

  @Override
  public String getQueueARN() throws JMSException {
    Map<String, String> queueAttrMap = null;
    try {
      queueAttrMap = _sqsQueue.getQueueAttributes(QueueAttribute.QUEUE_ARN);
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to get queue ARN for queue " + _sqsQueue.getUrl(), e);
    }
    return queueAttrMap.get(QueueAttribute.QUEUE_ARN.queryAttribute());
  }

  @Override
  public void setPolicy(String policy) throws JMSException {
    try {
      _sqsQueue.setQueueAttribute(QueueAttribute.POLICY.queryAttribute(), policy);
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to set policy", e);
    }
  }

  @Override
  public void deleteMessage(final String sqsReceiptHandle) throws JMSException {
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    Callable<Void> callable = new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        try {
          _sqsQueue.deleteMessage(sqsReceiptHandle);
        } catch (SQSException e) {
          throw _typicaSQSConnector.handleAWSException("Unable to delete message from queue " + _sqsQueue.getUrl(), e);
        }
        return null;
      }
    };
    Future<Void> result = executorService.submit(callable);
    try {
      result.get(60, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      e.printStackTrace();
    } catch (TimeoutException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      result.cancel(true);
      executorService.shutdown();
    }
  }

  @Override
  public TypicaSQSMessage receiveMessage() throws JMSException {
    TypicaSQSMessage sqsMessage;
    try {
      Message message = _sqsQueue.receiveMessage();
      sqsMessage = (message != null) ? new TypicaSQSMessage(message) : null;
    } catch (SQSException e) {
      throw _typicaSQSConnector.handleAWSException("Unable to retrieve message from queue " + _sqsQueue.getUrl(), e);
    }
    return sqsMessage;
  }
}
