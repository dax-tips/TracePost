using System;
using Microsoft.AnalysisServices.Tabular;
using System.Threading;
using System.Xml;
using Newtonsoft.Json;
using System.Net.Http;
using System.Threading.Tasks;
using System.Collections;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Identity.Client;
using System.IO;
using System.Linq;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;

namespace TracePost
{
    class Program
    {
        /**********************************************************************************************
            Open and edit the appsettings.json file to update the parameters

            databaseToTrace : This is the connection string of the AS database you wish to trace
                            : Example settings

                            Data source=powerbi://api.powerbi.com/v1.0/myorg/Trace Post Demo;Initial catalog=AdventureWorksDW
                            asazure://aspaaseastus2.asazure.windows.net/instancenamehere:rw

            eventHubConnectionString : This is the End Point for the Azure Event HUB to post to.  

            workspaceName   : This the the name of the workspace inside the tenant you wish to create the 
                              streaming dataset. 

                            : Example

                            Trace Post Demo
            
        **********************************************************************************************/
        static string _sessionId;
        static HttpClient client = new HttpClient();
        static int BatchSleepTime = 5000;
        static int rowsPerPost = 500;
        private const string pushDatasetName = "ASTracePushDataset";
        private const string pushDatasetTableName = "Trace";
        private const string eventHubName = "tracepost";
        static string workspaceId;
        static string pushDatasetId;

        static PBIHTTPHelper pbiHttpHelper;
        static Queue myQ = new Queue();

        static IConfigurationRoot configuration = new ConfigurationBuilder()
                        .SetBasePath(Directory.GetCurrentDirectory())
                        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true).Build();

        private static string workspaceName;
        static void Main(string[] args)
        {

            workspaceName = configuration.GetSection("WorkspaceNames:workspaceName").Value;
            pbiHttpHelper = new PBIHTTPHelper(interactiveLogin: true);

            EnsurePBIPushDataSet().Wait();

            Server server = new Server();
            Console.CursorVisible = false;

            int rowsProcessed = 0;

            /**************************************************************************
                Connect to Azure Analysis Services or Power BI Premium
                    PBI Premium requires XMLA-Read
                    PBI Premium also requires to connect to Server AND DB (using inital Catalog property)
                    This should enable MFA popup
            **************************************************************************/

            server.Connect(configuration.GetConnectionString("databaseToTrace"));
            _sessionId = server.SessionID;

            /**************************************************************************
                Remove any previous version of trace created using this app
            **************************************************************************/

            TraceCollection traceCollection = server.Traces;
            for (int d = 0; d < traceCollection.Count; d++)
            {
                Trace p = traceCollection[d];
                if (p.Name.StartsWith("TracePost"))
                {
                    if (p.IsStarted)
                    { p.Stop(); }
                    p.Drop();
                }
            }

            /**************************************************************************
                Create the trace
            **************************************************************************/

            Trace trace = server.Traces.Add("TracePost");

            trace.AutoRestart = true;

            /**************************************************************************
                Create the event 1 of 2 to trace [ProgressReportCurrent]
            **************************************************************************/

            TraceEvent traceEvent = trace.Events.Add(Microsoft.AnalysisServices.TraceEventClass.ProgressReportCurrent);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.IntegerData);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.CurrentTime);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectReference);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.SessionID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectName);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.DatabaseName);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.StartTime);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EventSubclass);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.TextData);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ActivityID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.RequestID);
            traceEvent.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ProgressTotal);

            /**************************************************************************
                Create the event 2 of 2 to trace [ProgressReportCurrent]
            **************************************************************************/
            TraceEvent traceEvent2 = trace.Events.Add(Microsoft.AnalysisServices.TraceEventClass.ProgressReportEnd);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.IntegerData);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.CurrentTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectReference);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.SessionID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ObjectName);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.DatabaseName);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.StartTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EndTime);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.EventSubclass);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.TextData);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.Duration);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ActivityID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.RequestID);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ProgressTotal);
            traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.Success);
            // traceEvent2.Columns.Add(Microsoft.AnalysisServices.TraceColumn.ApplicationName);            

            /**************************************************************************
                Determine the function to handle trace events as the happen
            **************************************************************************/

            trace.OnEvent += new TraceEventHandler(Trace_OnEvent);

            /**************************************************************************
                Save the trace
            **************************************************************************/

            trace.Update(Microsoft.AnalysisServices.UpdateOptions.Default, Microsoft.AnalysisServices.UpdateMode.CreateOrReplace);

            /**************************************************************************
                Start the trace
            **************************************************************************/

            trace.Start();

            /**************************************************************************
                Create an infinite loop that runs every 5 second to check for items
                store in the myQ queue. Post results in batches to the Power BI streaming 
                API endpoint
            **************************************************************************/

            Boolean b = true;
            do
            {

                /**************************************************************************
                    Pause for 5 seconds
                **************************************************************************/

                Thread.Sleep(BatchSleepTime);

                /**************************************************************************
                    Create a batch of rows from the myQ queue
                **************************************************************************/
                var rowsToSend = new List<myTraceEvent>();

                for (int i = 0; i < rowsPerPost && myQ.Count > 0; i++)
                {
                    rowsToSend.Add((myTraceEvent)myQ.Dequeue());
                    rowsProcessed++;
                }

                /**************************************************************************
                    If there were items in the queue send them to Power BI now
                **************************************************************************/

                if (rowsToSend.Count != 0)
                {
                    SendToPBIDataset(rowsToSend).Wait();
                }

                /**************************************************************************
                    Update screen to show progress
                **************************************************************************/

                Console.SetCursorPosition(10, 10);
                Console.Write($"Queue Count : {myQ.Count}     ");

                Console.SetCursorPosition(10, 12);
                Console.Write($"Rows Processed : {rowsProcessed}    ");

                Console.SetCursorPosition(10, 14);
                Console.Write($"Current Time : {DateTime.Now.ToString("yyyy-MM-dd hh:mm:ss")}    ");

            } while (b);


            trace.Stop();


        }


        static private void Trace_OnEvent(object sender, TraceEventArgs e)
        {

            if (e.ObjectName != null && e.ObjectReference != null && e.EventSubclass != Microsoft.AnalysisServices.TraceEventSubclass.Backup) // && e.SessionID == _sessionId)
            {
                XmlDocument document = new XmlDocument();
                document.LoadXml(e.ObjectReference);

                XmlNodeList tableNodeList = document.GetElementsByTagName("Table");
                XmlNodeList partitionNodeList = document.GetElementsByTagName("Partition");

                if (
                     e.EventClass == Microsoft.AnalysisServices.TraceEventClass.ProgressReportCurrent

                )
                {
                    string myTableName = tableNodeList[0].InnerText;
                    string myParitionName = partitionNodeList[0].InnerText;

                    string objectName = $"{myTableName}";
                    if (myTableName.ToUpper() != myParitionName.ToUpper() && myParitionName.ToUpper() != "PARTITION") { objectName += $":{myParitionName}"; }

                    myTraceEvent m = new myTraceEvent
                    {
                        CurrentTime = e.CurrentTime,
                        ObjectID = e.ObjectID,
                        ObjectName = objectName,
                        TableName = myTableName,
                        PartitionName = myParitionName,
                        EventSubClass = e.EventSubclass.ToString(),
                        DatabaseName = e.DatabaseName,
                        StartTime = e.StartTime,

                        EventClass = e.EventClass.ToString(),
                        SessionID = e.SessionID,
                        IntegerData = e.IntegerData,
                        LongObjectName = $"{e.SessionID}:{e.ObjectName}"
                    };
                    //string jsonString = $"{JsonConvert.SerializeObject(m)}";

                    myQ.Enqueue(m);
                    //SendToEventHub(jsonString);
                }

                if (
                    e.EventClass == Microsoft.AnalysisServices.TraceEventClass.ProgressReportEnd &&
                    (e.EventSubclass == Microsoft.AnalysisServices.TraceEventSubclass.ExecuteSql ||
                        e.EventSubclass == Microsoft.AnalysisServices.TraceEventSubclass.ReadData
                    )
                )
                {

                    string myTableName = tableNodeList[0].InnerText;
                    string myParitionName = partitionNodeList[0].InnerText;

                    string objectName = $"{myTableName}";
                    if (myTableName.ToUpper() != myParitionName.ToUpper() && myParitionName.ToUpper() != "PARTITION") { objectName += $":{myParitionName}"; }

                    myTraceEvent m = new myTraceEvent
                    {
                        CurrentTime = e.CurrentTime,
                        ObjectID = e.ObjectID,
                        ObjectName = objectName,
                        TableName = myTableName,
                        PartitionName = myParitionName,
                        Duration = (int)e.Duration,
                        EventSubClass = e.EventSubclass.ToString(),
                        DatabaseName = e.DatabaseName,
                        StartTime = e.StartTime,
                        EndTime = e.EndTime,
                        EventClass = e.EventClass.ToString(),
                        SessionID = e.SessionID,
                        IntegerData = e.IntegerData,
                        LongObjectName = $"{e.SessionID}:{e.ObjectName}"
                    };

                    myQ.Enqueue(m);

                }
            }
        }

        static async Task SendToEventHub(String message)
        {
            /**************************************************************************
                Only use if sending data to Azure EventHub
            **************************************************************************/

            EventHubProducerClient producer = new EventHubProducerClient(configuration.GetConnectionString("eventHubConnectionString"), eventHubName);

            try
            {
                using var eventBatch = await producer.CreateBatchAsync();

                BinaryData eventBody = new BinaryData(message);
                EventData eventData = new EventData(eventBody);

                if (!eventBatch.TryAdd(eventData))
                {
                    throw new Exception("The first event could not be added.");
                }

                await producer.SendAsync(eventBatch);
            }
            finally
            {
                await producer.CloseAsync();
            }
        }

        static async Task SendToPBIDataset(List<myTraceEvent> events)
        {
            var pushRowsObj = new JObject(new JProperty("rows"
               , JArray.FromObject(events)

           ));

            await pbiHttpHelper.ExecutePBIRequest($"datasets/{pushDatasetId}/tables/{pushDatasetTableName}/rows"
                , HttpMethod.Post
                , CancellationToken.None
                , body: pushRowsObj.ToString(Newtonsoft.Json.Formatting.None)
                , groupId: workspaceId
            );
        }

        static async Task EnsurePBIPushDataSet()
        {

            var workspaces = await pbiHttpHelper.ExecutePBIRequest("groups", HttpMethod.Get, CancellationToken.None);

            var workspace = workspaces.Data.FirstOrDefault(s => s["name"].Value<string>() == workspaceName);

            if (workspace == null)
            {
                Console.WriteLine($"Creating workspace: '{workspaceName}'");

                var workspaceResponse = await pbiHttpHelper.ExecutePBIRequest("groups", HttpMethod.Post
                    , CancellationToken.None
                    , body: new JObject(new JProperty("name", workspaceName)).ToString(Newtonsoft.Json.Formatting.None));

                if (workspaceResponse.Data == null)
                {
                    throw new Exception("Cannot create workspace");
                }

                workspace = workspaceResponse.Data;
            }

            workspaceId = workspace["id"].Value<string>();

            var datasets = await pbiHttpHelper.ExecutePBIRequest("datasets", HttpMethod.Get, CancellationToken.None, groupId: workspaceId);

            var dataset = datasets.Data.FirstOrDefault(s => s["name"].Value<string>() == pushDatasetName);

            if (dataset != null)
            {
                Console.WriteLine("Dataset already exists");
                pushDatasetId = dataset["id"].Value<string>();
                return;
            }

            var pbiJsonObj = new JObject(
                new JProperty("name", pushDatasetName)
                ,
                new JProperty("tables",
                    new JArray(
                        new JObject(
                            new JProperty("name", pushDatasetTableName)
                            ,
                            new JProperty("measures",
                                new JArray(
                                    new JObject(
                                        new JProperty("name", "Avg. Duration")
                                        ,
                                        new JProperty("expression", $"AVERAGE('{pushDatasetTableName}'[Duration])")
                                        ,
                                        new JProperty("formatString", "0.00")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "# Traces")
                                        ,
                                        new JProperty("expression", $"COUNTROWS('{pushDatasetTableName}')")
                                        ,
                                        new JProperty("formatString", "0")
                                    )
                                )
                            )
                            ,
                            new JProperty("columns"
                                ,
                                new JArray(
                                    new JObject(
                                        new JProperty("name", "CurrentTime")
                                        ,
                                        new JProperty("dataType", "DateTime")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "ObjectName")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "TableName")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "PartitionName")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "SessionID")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "ObjectID")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "Duration")
                                        ,
                                        new JProperty("dataType", "Int64")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "EventSubClass")
                                        ,
                                        new JProperty("dataType", "string")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "DatabaseName")
                                        ,
                                        new JProperty("dataType", "string")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "StartTime")
                                        ,
                                        new JProperty("dataType", "DateTime")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "EndTime")
                                        ,
                                        new JProperty("dataType", "DateTime")
                                    )
                                     ,
                                    new JObject(
                                        new JProperty("name", "EventClass")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                    ,
                                    new JObject(
                                        new JProperty("name", "IntegerData")
                                        ,
                                        new JProperty("dataType", "Int64")
                                    ),
                                    new JObject(
                                        new JProperty("name", "LongObjectName")
                                        ,
                                        new JProperty("dataType", "String")
                                    )
                                )
                            )
                        )
                    )
                )
            );

            var response = await pbiHttpHelper.ExecutePBIRequest("datasets"
                , HttpMethod.Post, CancellationToken.None
                , groupId: workspaceId
                , body: pbiJsonObj.ToString(Newtonsoft.Json.Formatting.None)
                );

            if (response.HttpError != null)
            {
                throw new Exception("Error ensuring PBI PUSH Dataset");
            }

            pushDatasetId = response.Data["id"].Value<string>();
        }

        public class myTraceEvent
        {
            public DateTime CurrentTime { get; set; }
            public string ObjectName { get; set; }
            public string TableName { get; set; }
            public string PartitionName { get; set; }
            public string ObjectID { get; set; }
            public int Duration { get; set; }
            public string EventSubClass { get; set; }
            public string DatabaseName { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime EndTime { get; set; }
            public string EventClass { get; set; }
            public long IntegerData { get; set; }
            public string SessionID { get; set; }
            public string LongObjectName { get; set; }


        }





    }
}