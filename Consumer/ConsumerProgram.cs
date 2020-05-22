using STAN.Client;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Consumidor
{
    public static class ConsumerProgram
    {

        static void Main(string[] args)
        {
            string CLUSTER_ID = "cluster-teste-local"; 
            string CLIENT_ID = "Consumer-" + DateTime.Now.Second;
            string DURABLE_NAME = "durable-queue";
            string SUBJECT = "subject-test";
            string QUEUE_GROUP = "queue-group-test";
            string URL_NATS = "nats://localhost:4222";


            var cf = new StanConnectionFactory();
            StanOptions stanOptions = StanOptions.GetDefaultOptions();
            stanOptions.NatsURL = URL_NATS;

            Console.WriteLine($"Connecting (URL_NATS = {URL_NATS}) ...");
            Console.WriteLine($"CLUSTER_ID = {CLUSTER_ID}");
            Console.WriteLine($"CLIENT_ID = {CLIENT_ID}");

            using (var c = cf.CreateConnection(CLUSTER_ID, CLIENT_ID, stanOptions))
            {

                var subscriptionOpts = StanSubscriptionOptions.GetDefaultOptions();
                                
                subscriptionOpts.DurableName = DURABLE_NAME;                
                subscriptionOpts.ManualAcks = true;
                subscriptionOpts.MaxInflight = 1;
                                
                var cts = new CancellationTokenSource();

                Task.Run(() =>
                {

                    c.Subscribe(SUBJECT, QUEUE_GROUP, subscriptionOpts, (obj, args) =>
                    {
                        string msg = Encoding.UTF8.GetString(args.Message.Data);

                        Console.WriteLine($"Msg received on {SUBJECT} : {msg}");

                        args.Message.Ack();
                    });


                }, cts.Token);



                Console.WriteLine("Waiting...  [press any key to exit]");
                Console.ReadKey();

                cts.Cancel();
            }


        }
    }
}
