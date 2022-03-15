using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace edvan.function
{
    public class eh_to_synapse
    {
        [FunctionName("eh_to_synapse")]
        public async Task Run([EventHubTrigger("ha", Connection = "edvanhomeeh_ha_EVENTHUB")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
// {"entity_id": "sun.sun", "state": "above_horizon", "attributes": {"next_dawn": "2022-03-16T05:20:19.846754+00:00", "next_dusk": "2022-03-15T18:20:55.651791+00:00", "next_midnight": "2022-03-15T23:50:58+00:00", "next_noon": "2022-03-15T11:51:22+00:00", "next_rising": "2022-03-16T05:54:34.833369+00:00", "next_setting": "2022-03-15T17:46:34.071684+00:00", "elevation": 24.91, "azimuth": 131.43, "rising": true, "friendly_name": "Sun"}, "last_changed": "2022-03-15T05:56:53.640871+00:00", "last_updated": "2022-03-15T08:59:41.692485+00:00", "context": {"id": "613f127b2230326f0a0e3e578f3b9dd3", "parent_id": null, "user_id": null}}
// {"entity_id": "sensor.power_production_w_2", "state": "0.0", "attributes": {"unit_of_measurement": "W", "device_class": "power", "icon": "mdi:flash", "friendly_name": "Power Production (W)"}, "last_changed": "2022-03-15T08:59:40.401520+00:00", "last_updated": "2022-03-15T08:59:40.401520+00:00", "context": {"id": "f4442a4dd7fd23e63fd53a3423441ba9", "parent_id": null, "user_id": null}}
                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
