using STAN.Client;
using System;
using System.Text;
using System.Threading;

namespace Produtor
{
    public static class ProducerProgram
    {
               
        //  .\nats-streaming-server.exe -cid cluster-teste-local -store file -dir dados-cluster-teste-local

        static void Main(string[] args)
        {
            string CLUSTER_ID = "cluster-teste-local";
            string CLIENT_ID = "Producer-"+DateTime.Now.Second;
            string SUBJECT = "subject-test";            
            string URL_NATS = "nats://localhost:4222";

            var cf = new StanConnectionFactory();
            StanOptions stanOptions = StanOptions.GetDefaultOptions();
            stanOptions.NatsURL = URL_NATS;

            Console.WriteLine($"Connecting (URL_NATS = {URL_NATS}) ...");
            Console.WriteLine($"CLUSTER_ID = {CLUSTER_ID}");
            Console.WriteLine($"CLIENT_ID = {CLIENT_ID}");


            using (var c = cf.CreateConnection(CLUSTER_ID, CLIENT_ID, stanOptions))
            {

                Random r = new Random();

                while (true)
                {
                    var msgId = r.Next();

                    string msg = CLIENT_ID + "-" + msgId.ToString();
                    c.Publish(SUBJECT, Encoding.UTF8.GetBytes(msg));

                    Console.WriteLine($"Msg published on {SUBJECT} : {msg}");

                    Thread.Sleep(1000);
                }                
                
            }

        }
    }
}
