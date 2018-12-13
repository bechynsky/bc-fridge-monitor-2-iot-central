using System;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Threading;
using System.Collections.Generic;

using Microsoft.Azure.Devices.Client;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.Binder;

using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

using Newtonsoft.Json;
using System.Globalization;

namespace bc_fridge_monitor_2_iot_central
{
   class Program
    {
        private static double _lastTemperature = 0.0;
        private static double _lastSetTemperature = 0.0;

        private static MqttClient _mqttClient = new MqttClient("localhost");
        
        private static IConfiguration _configuration { get; set; }

        private static List<DeviceClient> _devices = new List<DeviceClient>();
        
        private static AppConfig _ac = null;
        static void Main(string[] args)
        {
            CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
            /*
                Create connection string
                https://docs.microsoft.com/en-us/azure/iot-central/concepts-connectivity

                cd  /usr/local/lib/node_modules/dps-keygen/bin/linux
                ./dps_cstr dps_cstr <scope_id> <device_id> <Primary Key(for device)>
             */
             
            /*
                connection.json

                {
                    "AppConfig":{
                        "connectionStrings":[
                                "HostName=...",
                                "HostName=...",
                                ...
                        ]
                    }
                }
             */
            var builder = new ConfigurationBuilder()
                                .SetBasePath(Directory.GetCurrentDirectory())
                                .AddJsonFile("connection.json");
            _configuration = builder.Build();
            _ac = _configuration.GetSection("AppConfig").Get<AppConfig>();

            foreach (string cs in _ac.ConnectionStrings)
            {
                _devices.Add(DeviceClient.CreateFromConnectionString(cs));
            }

            _mqttClient.MqttMsgPublishReceived += Client_MqttMsgPublishReceived;

            _mqttClient.Connect("iotcentralgateway");
            
            if (_mqttClient.IsConnected)
            {
                _mqttClient.Subscribe(new string[] { "#" }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE });
                Console.WriteLine("MQTT connected");
            }

            while (true) {}
        }

        private static async void Client_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {
            string[] topicParts = e.Topic.Split('/');
            
            if (topicParts.Length < 3)
            {
                return;
            }

            string device = topicParts[1];
            string sensor = topicParts[2];
            string value = System.Text.Encoding.Default.GetString(e.Message);

            string data = "";
            int i1 = device.IndexOf(':');
            if (i1 <  0)
            {
                return;
            }

            int deviceIndex = int.Parse(device.Substring(i1 + 1));

            DeviceClient dc = _devices[deviceIndex];

            if (topicParts.Length == 5 &&
                device.StartsWith("fridge-monitoring") && 
                sensor.Equals("thermometer"))
            {
                string sensorInfo = topicParts[3];
                string measurement = topicParts[4];
            
                if (measurement.Equals("temperature") && 
                    sensorInfo.Equals("0:0"))
                {
                    _lastTemperature = double.Parse(value);
                    data = $"{{\"temperature\":{_lastTemperature}}}";
                } 
                
               
            }
            else if (topicParts.Length == 3 &&
                (sensor.Equals("high") || sensor.Equals("low")))
            {
                _lastTemperature = double.Parse(value);
                data = $"{{\"{sensor}\":{_lastTemperature}}}";
            }

            if (data.Length > 0)
            {
                Message payload = new Message(System.Text.Encoding.UTF8.GetBytes(data));
                await dc.SendEventAsync(payload);
                
                Console.Write(DateTime.Now.ToString());
                Console.Write(";");
                Console.Write(device);
                Console.Write(";");
                Console.WriteLine(data);            
            }
        }
    }

    class AppConfig
    {
        public string[] ConnectionStrings { get; set; }
    }
}
