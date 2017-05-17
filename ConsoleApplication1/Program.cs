using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
namespace ConsoleApplication1
{
    class Program
    {
        static string strreqQueue = "ReqQueue";
        static string strresQueue = "ResQueue";
        static string connection = "Endpoint=sb://wasifbus.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=iIEQztl3IJfr8cfoBvS6CydqJnf8dAZ9hnVhBOOa/3I=";
        static void Main(string[] args)
        {
            //factory instead of namespace
            // var factory = MessagingFactory.CreateFromConnectionString(connection);
            var ns = NamespaceManager.CreateFromConnectionString(connection);
            QueueDescription reqQD = CreateQueueDescription(false, strreqQueue);
            QueueDescription resQD = CreateQueueDescription(true, strresQueue);
            if (!ns.QueueExists(strreqQueue))
            {
                ns.CreateQueue(reqQD);
            }
            if (!ns.QueueExists(strresQueue))
            {
                ns.CreateQueue(resQD);
            }
            Console.WriteLine("Press 1 for Order Creator,2 For order processor");
            var input = Console.ReadLine();
            if(input.Equals("1"))
            {
                OrderSender();
            }
            else if(input.Equals("2"))
            {
                OrderReceiver();
            }
            
        }

        private static void OrderReceiver()
        {
            var factory = MessagingFactory.CreateFromConnectionString(connection);
            var reqQueue = factory.CreateQueueClient(strreqQueue);
            var resQueue = factory.CreateQueueClient(strresQueue);
            reqQueue.OnMessage(msg => {
                var sessionId = msg.ReplyToSessionId;
                Console.WriteLine("Message receive with ID " + sessionId + " Product: " + msg.Label);
                var resMessage = new BrokeredMessage("Response of order produt:" + msg.Label);
                resMessage.Label = "Processed :" + msg.Label;
                resMessage.SessionId = sessionId;
                resQueue.Send(resMessage);
                Console.WriteLine("Response send");
            },new OnMessageOptions() { MaxConcurrentCalls = 10 });
            Console.ReadLine();
        }

        private static void OrderSender()
        {
            var factory = MessagingFactory.CreateFromConnectionString(connection);
            var reqQueue = factory.CreateQueueClient(strreqQueue);
            var resQueue = factory.CreateQueueClient(strresQueue);
            while (true)
            {
                Console.WriteLine("Press e for exit or write ProductName to order");
                var input = Console.ReadLine();
                if(input.Equals("e"))
                {
                    break;
                }
                else
                {
                    var guid = Guid.NewGuid();
                    BrokeredMessage msg = new BrokeredMessage(input);
                    msg.ReplyToSessionId = guid.ToString();
                    msg.Label = input; ;
                    reqQueue.Send(msg);
                    var msSession = resQueue.AcceptMessageSession(guid.ToString());
                    var resMessage=msSession.Receive();
                    if(resMessage!=null)
                    {
                        Console.WriteLine("Response of your message:" + resMessage.Label);
                    }
                    else
                    {
                        Console.WriteLine("Message receiving failed");
                    }
                    msSession.Close();
                }
            }
            factory.Close();
        }

        private static QueueDescription CreateQueueDescription(bool sessionEnabled, string queueName)
        {
            var qd = new QueueDescription(queueName)
            {
                RequiresSession = sessionEnabled,
                AutoDeleteOnIdle = TimeSpan.FromMinutes(5)

            };
            return qd;
        }
    }
}
